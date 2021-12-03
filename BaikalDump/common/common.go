package common

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"bytes"
	"strconv"
	"strings"
)

func ToStr(strObj interface{}) string {
	switch v := strObj.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", strObj)
	}
}


func ToInt(intObj interface{}) int64 {
	// 假定int == int64，运行在64位机
	switch v := intObj.(type) {
	case []byte:
		return ToInt(string(v))
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		if v > math.MaxInt64 {
			//info := fmt.Sprintf("ToInt, error, overflowd %v", v)
			//fmt.Println(info)
			return 0
		}
		return int64(v)
	case float32:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		strv := v
		if strings.Contains(v, ".") {
			strv = strings.Split(v, ".")[0]
		}
		if strv == "" {
			return 0
		}
		if intv, err := strconv.ParseInt(strv,10, 64); err == nil {
			return int64(intv)
		}
	}
	//	fmt.Printf(fmt.Sprintf("ToInt err, %v, %v not supportted\n", intObj,
	//		reflect.TypeOf(intObj).Kind()))
	return 0
}

func ToUint(intObj interface{}) uint64 {
	// 假定int == int64，运行在64位机
	switch v := intObj.(type) {
	case []byte:
		return ToUint(string(v))
	case int:
		return uint64(v)
	case int8:
		return uint64(v)
	case int16:
		return uint64(v)
	case int32:
		return uint64(v)
	case int64:
		return uint64(v)
	case uint:
		return uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return uint64(v)
	case float32:
		return uint64(v)
	case float64:
		return uint64(v)
	case string:
		strv := v
		if strings.Contains(v, ".") {
			strv = strings.Split(v, ".")[0]
		}
		if strv == "" {
			return 0
		}
		if intv, err := strconv.ParseUint(strv,10, 64); err == nil {
			return uint64(intv)
		}
	}
	//	fmt.Printf(fmt.Sprintf("ToInt err, %v, %v not supportted\n", intObj,
	//		reflect.TypeOf(intObj).Kind()))
	return 0
}


func CheckFileExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func ListDir(dirPath string) []string {
	dir, err := ioutil.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}
	var result []string
	for _, file := range dir {
		result = append(result, file.Name())
	}
	return result
}

func JsonToMap(jsonStr string) (map[string]interface{}, error) {
	var personFromJSON interface{}
//	m := make(map[string]interface{})
	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	decoder.UseNumber()
	err := decoder.Decode(&personFromJSON)
//	err := json.Unmarshal([]byte(jsonStr), &m)
	if err != nil {
		fmt.Printf("Unmarshal with error: %+v, len=[%v]\n", err, len(jsonStr))
		return nil, err
	}
	m := personFromJSON.(map[string]interface{})
	return m, nil
}

type RowItemType int

const (
	MAP 	 RowItemType = 0
	JSON     RowItemType = 1
	SQL		RowItemType = 2
)

type TableRowString struct{
	Table string
	RowStr string
	RowMap map[string]interface{}
	Sql string
	Type RowItemType
}
