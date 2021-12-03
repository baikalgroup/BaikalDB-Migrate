package main

import (
	"runtime"
	"os"
	"fmt"
	"sync"
	"BaikalDump/common"
	"BaikalDump/module"
)


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
	var wg sync.WaitGroup
	multiDumper := module.NewMultiDumper(&wg, config)
	multiDumper.Start()
	wg.Wait()
}
