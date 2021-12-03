package common

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

type DestMySQLConfig struct {
	Host string `yaml:"host"`
	Port int `yaml:"port"`
	User string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	WorkerCount int `yaml:"workercount"`
	CountOnceInsert int `yaml:"countpersql"`
	SqlMode string `yaml:"sqlmode"`
}
type LocalConfig struct {
	DumpDir string `yaml:"dumpdir"`
	Format string `yaml:"format"`
}
type Config struct {
	Table string `yaml:"table"`
	FilterTable string `yaml:"filtertable"`
	DBHost string `yaml:"host"`
	DBPort int `yaml:"port"`
	DBUser string `yaml:"user"`
	DBPassword string `yaml:"password"`
	Database string `yaml:"database"`
	MainCpuCount int `yaml:"cpucount"`
	StepSize int `yaml:"stepsize"`
	WorkerCount int `yaml:"workercount"`
	LocalConfig LocalConfig `yaml:"localconfig"`
	Type string `yaml:"type"`
	DestMySQL DestMySQLConfig `yaml:"destmysql"`
}


func GetLocalConfig(fileName string)(config *Config){
	yamlFile, _ := ioutil.ReadFile(fileName)
	err := yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		fmt.Println("parse config faild!", err.Error(), fileName)
		os.Exit(1)
	}
	return  config
}
