package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"sync"
	"BaikalDump/common"
	"BaikalDump/module"
)

func LoadLocalData(dirName string, outQue chan *common.TableRowString, wg * sync.WaitGroup, ty string){
	fileList := common.ListDir(dirName)
	defer close(outQue)
	for _, fileName := range fileList {
		fi, err := os.Open(path.Join(dirName, fileName))
		if err != nil {
			panic(err)
			continue
		}
		defer fi.Close()
		br := bufio.NewReader(fi)
		lineCount := 0
		for {
			var line string
			var c error
			var isPrefix bool
			var a []byte
			for true {
				a, isPrefix, c = br.ReadLine()
				line += string(a)
				if isPrefix == false || c == io.EOF {
					break
				}
			}
			if c == io.EOF {
				break
			}
			t := &common.TableRowString {
				Table: fileName,
			}
			if ty == "sql"{
				t.Sql = line
			}else {
				t.RowStr = line
			}
			outQue <- t
			lineCount ++
			if lineCount % 10000 == 0 {
				fmt.Printf("load table[%v], lines[%v]\n", fileName, lineCount)
			}
		}
		fmt.Printf("load table[%v] done, lines[%v]\n", fileName, lineCount)
	}
	wg.Done()
}

func main(){
	configFileName := "./conf/config.yaml"
	if len(os.Args) > 1 {
	    configFileName = os.Args[1]
	    if common.CheckFileExists(configFileName) == false {
		fmt.Println("config file not exists!", configFileName)
		os.Exit(1)
	    }
	}
	config := common.GetLocalConfig(configFileName)
	runtime.GOMAXPROCS(config.MainCpuCount)
	rowQue := make(chan *common.TableRowString, 2000)
	var wg sync.WaitGroup
	wg.Add(1)
	go LoadLocalData(config.LocalConfig.DumpDir, rowQue, &wg, config.LocalConfig.Format)
	writer := module.NewMysqlWriter(rowQue, &wg, config.DestMySQL, config.LocalConfig.Format)
	writer.Start()
	wg.Wait()
}
