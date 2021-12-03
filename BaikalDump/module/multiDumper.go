package module

import (
	"baikalDump/common"
	"baikalDump/tools"
	"fmt"
	"os"
	"strings"
	"sync"
)

type TableRegion struct {
	table string
	region int
}

type MultiDumper struct {
	dbClient *tools.DBClient
	outQueue chan *common.TableRowString
	wg *sync.WaitGroup
	primaryKeys []string
	endFilterStr	string
	pkTupleStr	string
	valuePrepareStr string
	dumperWaitGroup sync.WaitGroup
	tableRegionMap map[string] map[int] bool
	dumperList [] *Dumper
	tableRegionQueue chan *TableRegion
	tableList [] string
	config *common.Config
	filterTableMap map[string]bool
}

func NewMultiDumper(wg *sync.WaitGroup, config *common.Config)(*MultiDumper){
	dbClient, err := tools.CreateDBClient(config.DBHost, config.DBPort, config.DBUser, config.DBPassword, config.Database)
	if err != nil {
		panic(err)
		os.Exit(1)
	}

	dumper := &MultiDumper{
		dbClient: dbClient,
		wg: wg,
		outQueue: make(chan *common.TableRowString,100000),
		dumperWaitGroup: sync.WaitGroup{},
		tableList: make([]string, 0),
		tableRegionQueue: make(chan *TableRegion, 10000),
		config: config,
		filterTableMap: make(map[string]bool),
	}
	filterTableStr := config.FilterTable
	filterTableList := strings.Split(filterTableStr, ",")
	for _, ftb := range filterTableList{
		dumper.filterTableMap[ftb] = true
	}
	return dumper
}

func (self *MultiDumper)getAllTableRegions()( map[string]map[int]bool){
	tableRegionMap := make(map[string]map[int] bool, 0)
	for _, table := range self.tableList{
		if ok, _ := self.filterTableMap[table]; ok {
			continue
		}
		regionList, err := self.dbClient.GetRegionList(table)
		if err != nil {
			fmt.Sprintf("get region list faild ! table=[%v]", table)
			os.Exit(1)
		}
		if len(regionList) <= 0 {
			continue
		}
		tableRegionMap[table] = make(map[int]bool)
		for _, region := range regionList {
			tableRegionMap[table][region] = true
		}
	}
	return tableRegionMap
}

func (self *MultiDumper)initTableRegionList(){
	self.tableRegionMap = make(map[string]map[int] bool, 0)
	if self.config.Table != "" {
		self.tableList = strings.Split(self.config.Table, ",")
	}else {
		var err error
		self.tableList, err = self.dbClient.GetTableList()
		if err != nil {
			fmt.Println("get table list faild!")
			os.Exit(1)
		}
	}
	self.tableRegionMap = self.getAllTableRegions()

	for table, tableRegion := range self.tableRegionMap{
		for region,_ := range tableRegion {
			t := &TableRegion{
				table:  table,
				region: region,
			}
			self.tableRegionQueue <- t
			self.dumperWaitGroup.Add(1)
			fmt.Println("put table region:", t.table, t.region)
		}
	}
}

func (self *MultiDumper)waitDumperDone(){
	defer close(self.outQueue)
	defer close(self.tableRegionQueue)
	for true {
		self.dumperWaitGroup.Wait()
		tableRegionDict := self.getAllTableRegions()
		hasChange := false
		for table, tableRegion := range tableRegionDict {
			if _, ok := self.tableRegionMap[table]; !ok{
				continue
			}
			for region, _ := range tableRegion {
				if _, ok := self.tableRegionMap[table][region]; !ok {
					hasChange = true
					t := &TableRegion{
						table: table,
						region: region,
					}
					fmt.Println("add table region:", t.table, t.region)
					self.tableRegionQueue <- t
					self.dumperWaitGroup.Add(1)
					self.tableRegionMap[table][region] = true
				}
			}
		}
		if hasChange == false {
			break
		}
	}
	self.wg.Done()
}

func (self *MultiDumper)Start(){
	self.initTableRegionList()
	idx := 0
	if self.config.WorkerCount < 0 {
		self.config.WorkerCount = 8
	}
	if self.config.WorkerCount > 64 {
		self.config.WorkerCount = 64
	}

	if self.config.StepSize < 0 {
		self.config.StepSize = 1000
	}
	if self.config.StepSize > 100000 {
		self.config.StepSize = 100000
	}
	dumpType := "row"
	if self.config.Type == "local"{
		dumpType = self.config.LocalConfig.Format
		if dumpType != "sql"{
		    dumpType = "json"
		}
	}
	for idx < self.config.WorkerCount {
		dumper := NewDumper(self.dbClient, &self.dumperWaitGroup, self.tableRegionQueue, self.config.StepSize, self.outQueue, dumpType)
		self.dumperList = append(self.dumperList, dumper)
		idx += 1
	}
	for _, dumper := range self.dumperList {
		dumper.Start()
	}

	self.StartWriter()
	self.startWaitDumperDone()
}

func (self *MultiDumper)startLocalWriter(){
	LocalWriter := NewLocalWriter(self.outQueue, self.wg, self.config.LocalConfig.DumpDir)
	LocalWriter.Start()
}

func (self *MultiDumper)StartWriter(){
	if self.config.Type == "local"{
		self.startLocalWriter()
	}else if self.config.Type == "mysql" {
		self.startMysqlWriter()
	}
}

func (self *MultiDumper)startMysqlWriter(){
	writer := NewMysqlWriter(self.outQueue, self.wg, self.config.DestMySQL, "map")
	writer.Start()
}

func (self *MultiDumper)startWaitDumperDone(){
	self.wg.Add(1)
	go self.waitDumperDone()
}
