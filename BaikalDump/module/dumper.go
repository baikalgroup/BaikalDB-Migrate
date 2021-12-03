package module

import (
	"baikalDump/common"
	"baikalDump/tools"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
)
type Dumper struct {
	dbClient       *tools.DBClient
	wg             *sync.WaitGroup
	tableRegionQue	chan *TableRegion
	outQue			chan *common.TableRowString
	curStep	int
	stepSize int
	curPks		[] string
	curPkString string
	endPoint	map[string] interface{}
	primaryKeys [] string
	primaryDict map[string] bool
	pkTupleStr	string
	valuePrepareStr string
	endFilterStr string
	table string
	region int
	wtype string
}


func NewDumper(dbClient * tools.DBClient, wg *sync.WaitGroup, tableRegionQue chan *TableRegion, stepSize int, outQueue chan *common.TableRowString, wtype string)(*Dumper) {
	dumper := &Dumper{
		dbClient:       dbClient,
		wg:             wg,
		tableRegionQue: tableRegionQue,
		outQue:         outQueue,
		curStep:        0,
		stepSize:       stepSize,
		wtype:			wtype,
	}
	return dumper
}

func (dumper *Dumper)InitTableRegion(table string, region int)(bool){
	dumper.table = table
	dumper.region = region
	dumper.primaryKeys, _ = dumper.dbClient.GetPrimaryKeys(table)
	dumper.primaryDict = make(map[string]bool)
	for _, pk := range dumper.primaryKeys{
		dumper.primaryDict[pk] = true
	}
	maxPk ,_ := dumper.dbClient.GetMaxPrimary(table, dumper.primaryKeys)
	if maxPk == nil {
		return false
	}
	dumper.endPoint = maxPk
	var pklistn [] string
	var valueList []string

	for i, _ := range dumper.primaryKeys {
		pk := dumper.primaryKeys[i]
		pklistn = append(pklistn, "`" + pk + "`")
		valueList = append(valueList, "?")

	}
	dumper.pkTupleStr = "(" + strings.Join(pklistn,",") + ")"
	dumper.valuePrepareStr = "(" + strings.Join(valueList, ",") + ")"


	dumper.endFilterStr = ""
	var endValue [] string
	for i, _ := range dumper.primaryKeys {
		pk := dumper.primaryKeys[i]
		va := dumper.endPoint[pk]
		endValue = append(endValue, "\"" + common.ToStr(va) + "\"")
	}
	dumper.endFilterStr = dumper.pkTupleStr + " <= (" + strings.Join(endValue, ",") + ")"
	return true
}

func (self *Dumper)Start(){
	go self.dump()
}

func (self *Dumper)dump(){
	for true {
		tbReg := <-self.tableRegionQue
		if tbReg == nil {
			break
		}
		fmt.Printf("dump start region=[%v] table=[%v]\n", tbReg.region, tbReg.table)
		beg := time.Now()
		self.dumpOneRegion(tbReg.table, tbReg.region)
		end := time.Now()
		fmt.Printf("dump done region=[%v] table=[%v] cost=[%v]\n", tbReg.region, tbReg.table, end.Sub(beg))
	}
}

func (self *Dumper)dumpOneRegion(table string, region int){
	defer self.wg.Done()
	ret := self.InitTableRegion(table, region)
	if ret == false {
		return
	}
	self.curStep = 0
	for true{
		if false == self.dumpOnce(){
			break
		}
		self.curStep += 1
	}
}
func (self * Dumper)putToQue(rows []*common.TableRowString)(bool){
	for _, row := range rows {
		self.outQue <- row
	}
	return true
}

func (self *Dumper)dumpOnce()(bool){
	var sql string

	if self.curStep == 0 {
		sql = fmt.Sprintf("/*{\"region_id\":%d}*/ select * from `%s` where %s limit %d", self.region, self.table, self.endFilterStr, self.stepSize)
	}else {
		sql = fmt.Sprintf("/*{\"region_id\":%d}*/ select * from `%s` where %s and %s > %s limit %d", self.region, self.table, self.endFilterStr, self.pkTupleStr, self.curPkString, self.stepSize)
	}
	fmt.Println("sql:", sql)

	result, columns, err := self.dbClient.QueryReturnColumns(sql)
	tryTime := 1
	for err != true {
		tryTime ++
		fmt.Printf("query faild! try again %v sql=[%v]", tryTime, sql)
		time.Sleep(time.Duration(5) * time.Second)
		result, columns, err = self.dbClient.QueryReturnColumns(sql)
	}
	if len(result) == 0{
		return false
	}

	tableRowList := self.ConstructTableRows(result, columns)

	self.putToQue(tableRowList)
	lastRow := result[len(result) - 1]
	lastRowMap := self.ColList2Map(columns, lastRow)
	self.curPkString = ""
	self.curPks = self.curPks[:0]
	for _, pk := range self.primaryKeys{
		self.curPks = append(self.curPks, "\"" + common.ToStr(lastRowMap[pk]) + "\"")
	}
	self.curPkString = "(" + strings.Join(self.curPks, ",") + ")";

	return true
}
func (self *Dumper)ConstructTableRows(rowsList [][]interface{}, columns []string)(result []*common.TableRowString){
	if self.wtype == "row" || self.wtype == "json" {
		for _, row := range rowsList{
			rowMap := make(map[string]interface{})
			for idx, colName := range columns{
				rowMap[colName] = row[idx]
			}
			tableRowItem := &common.TableRowString {
				Table: self.table,
			}
			if self.wtype == "row"{
				tableRowItem.RowMap = rowMap
				tableRowItem.Type = common.MAP

			}else {
				b, err := json.Marshal(rowMap)
				if err != nil {
					fmt.Println("parse row faild !", err.Error())
					panic(err)
				}
				tableRowItem.RowStr = string(b)
				tableRowItem.Type = common.JSON
			}
			result = append(result, tableRowItem)
		}
	}else if (self.wtype == "sql"){
		sqlList := self.ConstructSqlList(columns, rowsList)
		for _, sql := range sqlList{
			tableRowItem := &common.TableRowString{
				Table: self.table,
				Sql : sql,
				Type: common.SQL,
			}
			result = append(result, tableRowItem)
		}
	}
	return result
}

func (self *Dumper)ColList2Map(columns []string, valList []interface{})(map[string]interface{}){
	res := make(map[string]interface{})
	if len(columns) != len(valList){
	    fmt.Println(self.table, "columns != valList ", len(columns), "!=", len(valList))
	    os.Exit(1)
	}
	for idx, col := range columns{
		res[col] = valList[idx]
	}
	return res
}

func (self *Dumper)ConstructSqlList(columns []string, rowList [][]interface{})([]string){
	var result []string
	if len(rowList) == 0 {
		return result
	}
	var colsn []string

	for _, col := range columns{
		colsn = append(colsn, "`" + col + "`")
	}
	sqlPrefix := fmt.Sprintf("insert into `%s` (%s) values ", self.table, strings.Join(colsn, ","))

	curCnt := 0
	var rowStrList [] string
	for _, row := range rowList{
		var colStrList []string
		for _, col := range row{
			var colStr string
			if col == nil {
			    colStrList = append(colStrList, "NULL")
			    continue
			}
			colStr = common.ToStr(col)
			colType := reflect.TypeOf(col)
			switch colType.Kind() {
			case reflect.Uint8,reflect.Int8,reflect.Uint16,reflect.Int16,reflect.Uint32,reflect.Int32,
			reflect.Int, reflect.Uint, reflect.Int64, reflect.Uint64:
				break
			default:
				colStr = strings.Replace(colStr, "\\","\\\\", -1)
				colStr = strings.Replace(colStr, "\n","\\n", -1)
				colStr = strings.Replace(colStr, "'","\\'", -1)
				colStr = "'" + colStr + "'"
			}
			colStrList = append(colStrList, colStr)
		}
		curCnt ++
		rowStrList = append(rowStrList, fmt.Sprintf("(%s)", strings.Join(colStrList, ",")))
		if curCnt >= 100 {
			curCnt = 0
			sql := sqlPrefix + strings.Join(rowStrList,",")
			result = append(result, sql + ";")
			rowStrList = rowStrList[:0]
		}
	}
	if curCnt > 0 {
		sql := sqlPrefix + strings.Join(rowStrList,",")
		result = append(result, sql + ";")
	}
	return result
}
