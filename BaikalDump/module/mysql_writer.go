package module

import (
	"BaikalDump/common"
	"BaikalDump/tools"
	"fmt"
	"strings"
	"time"

	//	"fmt"
	"os"
	"sync"
)


type MysqlWriter struct {
	inQue chan *common.TableRowString
	wg *sync.WaitGroup
	dumpDir string
	wokerCount int
	dbClient *tools.DBClient
	subwg *sync.WaitGroup
	rowFormat string
	onceCount int
	config common.DestMySQLConfig
}


func NewMysqlWriter(inQue chan *common.TableRowString, wg *sync.WaitGroup, config common.DestMySQLConfig, rowFormat string)(*MysqlWriter){
	writer := &MysqlWriter{
		inQue: inQue,
		wg: wg,
		wokerCount: config.WorkerCount,
		subwg: &sync.WaitGroup{},
		rowFormat: rowFormat,
		onceCount : config.CountOnceInsert,
		config: config,
	}
	if writer.onceCount <= 0 {
		writer.onceCount = 1
	}
	if writer.onceCount > 1000 {
		writer.onceCount = 1000
	}
	return writer
}

func (self *MysqlWriter)Start(){
	self.wg.Add(1)
	go self.run()
}

func (self *MysqlWriter)run(){
	idx := 0
	for idx < self.wokerCount && idx < 32 {
		self.subwg.Add(1)
		go self.writeFunc()
		idx += 1
	}
	self.subwg.Wait()
	self.wg.Done()
}

func (self *MysqlWriter)writeFunc() {
	dbClient, err := tools.CreateDBClient(self.config.Host, self.config.Port, self.config.User,self.config.Password, self.config.Database)
	if err != nil {
		panic(err)
		os.Exit(1)
	}
	defer self.subwg.Done()
	lastTableName := ""
	var rowList []*common.TableRowString
	var keyMap map[string]bool
	listCount := 0
	for true {
		tableRow := <-self.inQue
		if tableRow == nil {
			break
		}
		if self.rowFormat == "json" {
			var err error
			tableRow.RowMap, err = common.JsonToMap(tableRow.RowStr)
			if err != nil {
				fmt.Println("parse json faild!", tableRow.RowStr)
				os.Exit(1)
			}
		}
		if tableRow.Table != lastTableName || listCount >= self.onceCount {
			self.multiInsertOnce(dbClient, lastTableName, keyMap, rowList)
			keyMap = make(map[string]bool)
			rowList = rowList[:0]
			lastTableName = tableRow.Table
			listCount = 0
		}
		for key, _ := range tableRow.RowMap {
			keyMap[key] = true
		}
		rowList = append(rowList, tableRow)
		listCount += 1
	}
	if listCount > 0 {
		self.multiInsertOnce(dbClient, lastTableName, keyMap, rowList)
	}
}

func (self *MysqlWriter)multiInsertOnce(dbClient *tools.DBClient, table string, keyMap map[string]bool, rowList []*common.TableRowString){
	if table == ""{
		return
	}
	if self.rowFormat == "sql" {
		for _, row := range rowList {
			sql := row.Sql
			for true {
				execRet, errMsg := dbClient.Execute(sql)
				if execRet == true {
					break
				}
				if strings.Contains(errMsg, "Error 1062: Duplicate entry:") {
					sql = strings.Replace(sql, "insert into", "replace into", 1)
				}
				time.Sleep(time.Duration(3) * time.Second)
			}
		}
		return
	}
	var keyList []string
	var keyList2 []string
	var holderList []string
	for key, _ := range keyMap {
		keyList = append(keyList, "`" + key + "`")
		holderList = append(holderList, "?")
		keyList2 = append(keyList2, key)
	}
	holderFormat := "(" + strings.Join(holderList, ",") + ")"
	var sqlPrefix string
	if self.config.SqlMode == "insert" {
		sqlPrefix = fmt.Sprintf("insert into %s (%s) values ", table, strings.Join(keyList, ","))
	}else{
		sqlPrefix = fmt.Sprintf("replace into %s (%s) values ", table, strings.Join(keyList, ","))
	}
	//fmt.Println("sql:",sqlPrefix)


	var sqlValueList []interface{}
	var holderFormatList []string

	for _, row := range rowList {
		holderFormatList = append(holderFormatList, holderFormat)
		for _, key := range keyList2 {
			if _, ok := row.RowMap[key]; ok {
				sqlValueList = append(sqlValueList, row.RowMap[key])
			} else {
				sqlValueList = append(sqlValueList, nil)
			}
		}
	}
	sql := sqlPrefix + strings.Join(holderFormatList,",")
	//fmt.Println(sql)
	for true {
		execRet, errMsg := dbClient.ExecuteArgs(sql, sqlValueList)
		if execRet == true{
			break
		}
		if strings.Contains(errMsg, "Error 1062: Duplicate entry:"){
			sql = strings.Replace(sql, "insert into", "replace into", 1)
		}
		time.Sleep(time.Duration(3) * time.Second)
	}
}


