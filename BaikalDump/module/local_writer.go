package module

import (
	"BaikalDump/common"
	"fmt"
	"os"
	"path"
	"sync"
)

type LocalWriter struct {
	inQue chan *common.TableRowString
	wg *sync.WaitGroup
	dumpDir string
}


func NewLocalWriter(inQue chan *common.TableRowString, wg *sync.WaitGroup, dumpDir string)(*LocalWriter){
	localWriter := &LocalWriter{
		inQue: inQue,
		wg: wg,
		dumpDir: dumpDir,
	}
	return localWriter
}

func (self *LocalWriter)Start(){
	self.wg.Add(1)
	go self.run()
}

func (self *LocalWriter)run(){
	tableFileMap := make(map[string]*os.File,0)
	countMap := make(map[string]int)
	for true {
		row := <- self.inQue
		if row == nil {
			break
		}
		table := row.Table
		if _, ok := tableFileMap[table]; !ok {
			var f *os.File
			var err1 error
			filePath := path.Join(self.dumpDir, table)
			f, err1 = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
			if err1 != nil {
				panic(err1)
				os.Exit(1)
			}
			tableFileMap[table] = f
			countMap[table] = 0
		}
		writeStr := row.RowStr
		if row.Type == common.SQL {
			writeStr = row.Sql
		}
		_, err := tableFileMap[table].WriteString(writeStr + "\n")
		countMap[table] += 1
		if countMap[table] % 10000 == 0 {
			fmt.Printf("%v write lines %v\n",table, countMap[table])
		}
		if err != nil {
			panic(err)
		}
	}
	for _, f := range tableFileMap{
		f.Close()
	}
	self.wg.Done()
}
