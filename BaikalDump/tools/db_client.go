package tools
 
import (
    gosql "database/sql"
    "errors"
    "fmt"
    _ "github.com/go-sql-driver/mysql"
    "os"
    "reflect"
    "strings"
    "time"
    "BaikalDump/common"
)

type DBClient struct {
    database string
    db * gosql.DB
}

func (dbClient *DBClient)PrepareExecute(sql string, valueList [] interface{})(bool, string){
    stmt, err := dbClient.db.Prepare(sql)
    if err != nil {
        return false, ""
    }
    _, err = stmt.Exec(valueList...)
    defer stmt.Close()
    if err != nil {
        fmt.Println("exec faild:", err.Error())
        return false, err.Error()
    }
    return true, ""
}

func (dbClient *DBClient)ExecuteArgs(sql string, valueList [] interface{}) (bool, string) {
    _, err := dbClient.db.Exec(sql, valueList...)
    if err != nil {
        fmt.Printf("exec faild, %v, %v\n", err.Error(), sql)
        return false, err.Error()
    }
    return true, ""
}

func (dbClient *DBClient)MultiExecutePrepare(preSql string, argsList [] []interface{}) bool {
    stmt, err := dbClient.db.Prepare(preSql)
    if err != nil {
        return false
    }

    for _, valueList := range(argsList){
        _, err = stmt.Exec(valueList...)
        if err != nil {
            fmt.Println("exec faild:", err.Error())
            return false
        }
    }
    stmt.Close()
    return true
}

func (dbClient *DBClient)Execute(sql string) (bool,string) {

    _, err := dbClient.db.Exec(sql)
    if err != nil {
        fmt.Printf("exec faild: sql = %v\n", sql)
        return false,err.Error()
    }
    return true,""
}

func (self *DBClient)PrepareQuery(sql string, valueList[] interface{}) ([]map[string]interface{}, error){
    stmt, err := self.db.Prepare(sql)
    if err != nil {
        fmt.Println("prepare faild!",err.Error())
        return nil, err
    }
    rows, err := stmt.Query(valueList...)
    list := RowsToMap(rows)
    _ = rows.Close()
    return list, nil
}

func CreateDBClient(host string, port int, user string, password string, database string) (*DBClient, error){
    dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4,utf8&interpolateParams=true&readTimeout=1m&multiStatements=true", user, password, host, port, database)
    db, err := gosql.Open("mysql", dsn)
    if err != nil {
	return nil, err
    }
    db.SetMaxOpenConns(32)
    db.SetMaxIdleConns(8)
    db.SetConnMaxIdleTime(time.Duration(60) * time.Second)
    dbClient := &DBClient {
	    db: db,
	    database: database,
    }
    return dbClient, nil
}


func (dbClient *DBClient)GetMaxMinPrimary(table string) (map[string]interface{}, map[string]interface{}, error) {
    pkList, err := dbClient.GetPrimaryKeys(table)
    if err != nil {
        panic(err)
    }
    minPk, err := dbClient.getMinPrimary(table, pkList)
    if err != nil {
        panic(err)
    }
    maxPk, err := dbClient.GetMaxPrimary(table, pkList)
    if err != nil {
        return nil, nil, errors.New("get max primary faild!")
    }
    return minPk, maxPk, nil
}


var BytesKind = reflect.TypeOf(gosql.RawBytes{}).Kind()
var TimeKind = reflect.TypeOf(gosql.NullTime{}).Kind()


func checkErr(err error) {
    if err != nil {
        fmt.Printf("checkErr:%v", err)
        panic(err)
    }
}

func RowsToMap(rows *gosql.Rows) []map[string]interface{} {
    result := make([]map[string]interface{}, 0)

    for rows.Next() {
        cols, err := rows.Columns()
        checkErr(err)

        colsTypes, err := rows.ColumnTypes()
        checkErr(err)

        dest := make([]interface{}, len(cols))
        destPointer := make([]interface{}, len(cols))
        for i := range dest {
            destPointer[i] = &dest[i]
        }

        err = rows.Scan(destPointer...)
        checkErr(err)

        rowResult := make(map[string]interface{})
        for i, colVal := range dest {
            colName := cols[i]
            itemType := colsTypes[i].ScanType()
            if colVal == nil {
                continue
            }
            switch itemType.Kind() {
            case BytesKind:
                rowResult[colName] = common.ToStr(colVal)

            case reflect.Int, reflect.Int8,
                reflect.Int16, reflect.Int32, reflect.Int64:

                rowResult[colName] = common.ToInt(colVal)

            case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
                rowResult[colName] = common.ToUint(colVal)
            case TimeKind:
                rowResult[colName] = common.ToStr(colVal)
            default:
                rowResult[colName] = common.ToStr(colVal)
            }
        }
        result = append(result, rowResult)
    }
    return result
}


func (dbClient *DBClient)Query(sql string) ([]map[string]interface{}, error){
    rows, err := dbClient.db.Query(sql)
    if err != nil {
        fmt.Println("query faild: sql = "  + sql + "\n err:" + err.Error())
        return nil, err
    }
    list := RowsToMap(rows)
    _ = rows.Close()
    return list, nil
}

func (dbClient *DBClient)QueryReturnColumns(sql string) ([][]interface{}, []string, bool) {
    rows, err := dbClient.db.Query(sql)
    if err != nil {
        return nil, nil, false
    }
    columns, err := rows.Columns()
    if err != nil {
        fmt.Println("get columns faild!", err.Error())
        return nil, nil, false
    }
    colsTypes, err := rows.ColumnTypes()
    if err != nil {
        fmt.Println("get columns type faild!", err.Error())
        return nil, nil, false
    }
    var retResult [][]interface{}
    for rows.Next(){
        dest := make([]interface{}, len(columns))
        destPointer := make([]interface{}, len(columns))
        for i := range dest {
            destPointer[i] = &dest[i]
        }

        err = rows.Scan(destPointer...)
        checkErr(err)
        var rowResult []interface{}
        for i, colVal := range dest {
            if colVal == nil {
                rowResult = append(rowResult, nil)
		continue
            }
            itemType := colsTypes[i].ScanType()
            switch itemType.Kind() {
            case BytesKind:
                rowResult = append(rowResult, common.ToStr(colVal))

            case reflect.Int, reflect.Int8,
                reflect.Int16, reflect.Int32, reflect.Int64:

                rowResult = append(rowResult,common.ToInt(colVal))

            case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
                rowResult = append(rowResult, common.ToUint(colVal))
            case TimeKind:
                rowResult = append(rowResult, common.ToStr(colVal))
            default:
                rowResult = append(rowResult, common.ToStr(colVal))
            }

        }
        retResult = append(retResult, rowResult)
    }
    return retResult, columns, true
}

func (dbClient *DBClient)loadFullTableData(table string)([]map[string]interface{}, error){
    sql := fmt.Sprintf("select count(*) from  %s", table);
    res, err := dbClient.Query(sql)
    if err != nil {
        return nil, err
    }
    return res, err
}

func (dbClient *DBClient)GetTableList()([]string, error){
    sql := "show tables;"
    //rows, err := dbClient.db.Query(sql)
    res, err := dbClient.Query(sql)
    if err != nil {
        return nil, err
    }
    var result [] string
    for _, row := range res {
        for _, v := range row {
            result = append(result, common.ToStr(v))
        }
    }
    return result, nil
}

func (dbClient *DBClient)GetRegionList(table string)([]int, error){

    sql := fmt.Sprintf("explain format = 'trace2' select 1 from `%s` limit 1;", table)
    rows, err := dbClient.Query(sql)
    if err != nil {
        return nil, err
    }
    var result [] int
    for _, row := range rows {
        regionId := common.ToInt(row["region_id"])
        if regionId != 0 {
            result = append(result, int(regionId))
        }
    }
    return result, nil
}


func (dbClient *DBClient) GetPrimaryKeys(table string)([] string, error){
    sql := fmt.Sprintf("select COLUMN_NAME from information_schema.STATISTICS " +
        "where TABLE_SCHEMA = \"%s\" and TABLE_NAME = \"%s\" " +
        " and INDEX_NAME = \"PRIMARY\" order by SEQ_IN_INDEX ",
        dbClient.database, table)

    rows, err := dbClient.db.Query(sql)
    if err != nil {
        fmt.Println("get pk faild! sql=", sql)
        panic(err)
        os.Exit(1)
    }
    result := make([] string, 0)
    for rows.Next() {
        var pk string
        err = rows.Scan(&pk)
        result = append(result, pk)
    }
    return result, nil
}

func (dbClient *DBClient)GetMaxPrimary(table string, pklist [] string)(map[string] interface {}, error){
    var keyListNew [] string
    for _, key := range pklist{
        keyListNew = append(keyListNew, "`" + key + "`")
    }
    keyListStr := strings.Join(keyListNew, ",")
    var descKeyList [] string
    for _, key := range keyListNew {
        descKeyList = append(descKeyList, key + " desc")
    }

    sqlMax := fmt.Sprintf("select %s from `%s` order by %s limit 1", keyListStr, table, strings.Join(descKeyList,","))

    res , err := dbClient.Query(sqlMax)
    if err != nil {
        panic(err)
    }
    if len(res) == 0 {
        return nil, nil
    }
    maxPk := res[0]
    return maxPk, nil
}

func (dbClient *DBClient)getMinPrimary(table string, pklist [] string)(map[string] interface {}, error){
    var keyListNew [] string
    for _, key := range pklist{
        keyListNew = append(keyListNew, "`" + key + "`")
    }
    keyListStr := strings.Join(keyListNew, ",")
    var descKeyList [] string
    for _, key := range keyListNew {
        descKeyList = append(descKeyList, key + " desc")
    }
    sqlMin := fmt.Sprintf("select %s from `%s` order by %s limit 1", keyListStr, table, keyListStr)
    res , err := dbClient.Query(sqlMin)
    if err != nil {
        panic(err)
    }
    if len(res) == 0 {
        return nil, nil
    }
    minPk := res[0]
    return minPk, nil
}


