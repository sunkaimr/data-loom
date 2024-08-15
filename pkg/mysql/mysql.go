package mysql

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"strconv"
	"strings"
	"time"
)

type Connect struct {
	Addr     string
	User     string
	Pass     string
	Database string
	Charset  string
	Conn     *sql.DB
}

func NewMysqlConnect(host, port, user, passwd, database string) (*Connect, error) {
	dsn := mysql.NewConfig()
	dsn.User = user
	dsn.Passwd = passwd
	dsn.Net = "tcp"
	dsn.Addr = fmt.Sprintf("%s:%s", host, port)
	dsn.DBName = database
	dsn.Timeout = time.Second * 3

	conn, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		return nil, err
	}
	connector := &Connect{
		Addr:     dsn.Addr,
		User:     dsn.User,
		Pass:     dsn.Passwd,
		Database: dsn.DBName,
		Conn:     conn,
	}
	return connector, err
}

func (db *Connect) Close() error {
	if db.Conn != nil {
		return db.Conn.Close()
	}
	return nil
}

// QueryResult 数据库查询返回值
type QueryResult struct {
	Rows    *sql.Rows
	Error   error
	Warning *sql.Rows
	//QueryCost float64
}

// ExplainInfo 用于存放Explain信息
type ExplainInfo struct {
	SQL         string
	ExplainRows []ExplainRow
	Warnings    []ExplainWarning
	//QueryCost     float64
}

// ExplainRow 单行Explain
type ExplainRow struct {
	ID           int
	SelectType   string
	TableName    string
	Partitions   string // explain partitions
	AccessType   string
	PossibleKeys []string
	Key          string
	KeyLen       string // 索引长度，如果发生了index_merge， KeyLen 格式为 N,N，所以不能定义为整型
	Ref          []string
	Rows         int64
	Filtered     float64 // 5.6 JSON, 5.7+, 5.5 EXTENDED
	Scalability  string  // O(1), O(n), O(log n), O(log n)+
	Extra        string
}

// ExplainWarning explain extended 后 SHOW WARNINGS 输出的结果
type ExplainWarning struct {
	Level   string
	Code    int
	Message string
}

// Query 执行SQL
func (db *Connect) Query(sql string, params ...interface{}) (QueryResult, error) {
	var res QueryResult
	var err error

	if db.Database == "" {
		db.Database = "information_schema"
	}

	_, err = db.Conn.Exec("USE `" + db.Database + "`")
	if err != nil {
		return res, err
	}
	res.Rows, res.Error = db.Conn.Query(sql, params...)

	// SHOW WARNINGS 并不会影响 last_query_cost
	//res.Warning, err = db.Conn.Query("SHOW WARNINGS")

	//cost, err := db.Conn.Query("SHOW SESSION STATUS LIKE 'last_query_cost'")
	//if err == nil {
	//	var varName string
	//	if cost.Next() {
	//		err = cost.Scan(&varName, &res.QueryCost)
	//		common.LogIfError(err, "")
	//	}
	//	if err := cost.Close(); err != nil {
	//		common.Log.Error(err.Error())
	//	}
	//}

	if res.Error != nil && err == nil {
		err = res.Error
	}
	return res, err
}

// ShowDatabases 查询库列表
func (db *Connect) ShowDatabases() ([]string, error) {
	res, err := db.Query("SHOW DATABASES;")
	if err != nil {
		return nil, fmt.Errorf("exec sql query failed, %s", err)
	}

	var rows []string
	for res.Rows.Next() {
		r := ""
		err = res.Rows.Scan(&r)
		if err != nil {
			return nil, fmt.Errorf("scan rows failed, %s", err)
		}
		rows = append(rows, r)
	}
	err = res.Rows.Close()
	if err != nil {
		return rows, fmt.Errorf("close scan rows failed, %s", err)
	}
	return rows, err
}

// ShowTables 查询表列表
func (db *Connect) ShowTables(database string) ([]string, error) {
	db.Database = database
	res, err := db.Query("SHOW TABLES;")
	if err != nil {
		return nil, fmt.Errorf("exec sql query failed, %s", err)
	}

	var rows []string
	for res.Rows.Next() {
		r := ""
		err = res.Rows.Scan(&r)
		if err != nil {
			return nil, fmt.Errorf("scan rows failed, %s", err)
		}
		rows = append(rows, r)
	}
	err = res.Rows.Close()
	if err != nil {
		return rows, fmt.Errorf("close scan rows failed, %s", err)
	}
	return rows, err
}

func TestMySQLConnect(host, port, user, passwd, database string) error {
	// 校验mysql连接信息
	m, err := NewMysqlConnect(host, port, user, passwd, database)
	if err != nil {
		return fmt.Errorf("new mysql connect failed, %s", err)
	}
	if _, err = m.ShowDatabases(); err != nil {
		return fmt.Errorf("connect mysql failed, %s", err)
	}
	return nil
}

// TablesHasPrimaryKey 表是否有主键
func (db *Connect) TablesHasPrimaryKey(database string, tables []string) (bool, error) {
	var noPrimaryKeyTabs []string
	for _, t := range tables {
		columnsConstraints, err := db.TableConstraints(database, t)
		if err != nil {
			return false, fmt.Errorf("get table rows failed, %s", err)
		}

		for _, constraints := range columnsConstraints {
			for _, cc := range constraints {
				if cc == "PRIMARY KEY" {
					goto next
				}
			}
		}
		noPrimaryKeyTabs = append(noPrimaryKeyTabs, t)
	next:
	}

	if len(noPrimaryKeyTabs) != 0 {
		return false, fmt.Errorf("tables %v has no PrimaryKey", noPrimaryKeyTabs)
	}
	return true, nil
}

// TableConstraints 查询表包含哪些约束
func (db *Connect) TableConstraints(d, table string) (map[string][]string, error) {
	var err error
	sqlQuery := fmt.Sprintf("SELECT "+
		"CONSTRAINT_NAME, CONSTRAINT_TYPE "+
		"FROM information_schema.TABLE_CONSTRAINTS "+
		"WHERE "+
		"TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", d, table)
	res, err := db.Query(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("exec sql query failed, %s", err)
	}
	constraint := make(map[string]string, 5)
	constraintName, constraintType := "", ""
	for res.Rows.Next() {
		err = res.Rows.Scan(&constraintName, &constraintType)
		if err != nil {
			return nil, fmt.Errorf("scan rows failed, %s", err)
		}
		constraint[constraintName] = constraintType
	}
	err = res.Rows.Close()
	if err != nil {
		return nil, fmt.Errorf("close scan rows failed, %s", err)
	}

	sqlQuery = fmt.Sprintf("SELECT "+
		"CONSTRAINT_NAME, COLUMN_NAME "+
		"FROM information_schema.KEY_COLUMN_USAGE "+
		"WHERE "+
		"TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", d, table)
	res, err = db.Query(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("exec sql query failed, %s", err)
	}

	columnName := ""
	col := make(map[string][]string, 5)
	for res.Rows.Next() {
		err = res.Rows.Scan(&constraintName, &columnName)
		if err != nil {
			return col, fmt.Errorf("scan rows failed, %s", err)
		}
		if _, ok := col[columnName]; ok {
			col[columnName] = append(col[columnName], constraint[constraintName])
		} else {
			col[columnName] = make([]string, 0, 1)
			col[columnName] = append(col[columnName], constraint[constraintName])
		}
	}
	err = res.Rows.Close()
	if err != nil {
		return col, fmt.Errorf("close scan rows failed, %s", err)
	}

	return col, err
}

// GetColumns 查询表列
func (db *Connect) GetColumns(database, table string) ([]string, error) {
	db.Database = database
	res, err := db.Query(
		fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = `%s` AND TABLE_NAME = `%s`;", database, table))
	if err != nil {
		return nil, fmt.Errorf("exec sql query failed, %s", err)
	}

	var rows []string
	for res.Rows.Next() {
		r := ""
		err = res.Rows.Scan(&r)
		if err != nil {
			return nil, fmt.Errorf("scan rows failed, %s", err)
		}
		rows = append(rows, r)
	}
	err = res.Rows.Close()
	if err != nil {
		return rows, fmt.Errorf("close scan rows failed, %s", err)
	}
	return rows, err
}

// TableSize 查询表大小
func (db *Connect) TableSize(database, table string) (int, error) {
	var err error
	// DATA_LENGTH: 已分配的数据空间的大小
	// INDEX_LENGTH: 已分配的索引空间的大小
	// DATA_FREE: 表中为数据保留的未使用空间的大小, 当数据被删除或更新后，MySQL存储引擎并不会立即回收相应的空间，而是将其标记为未使用状态
	sqlQuery := fmt.Sprintf("SELECT round(((DATA_LENGTH + INDEX_LENGTH + DATA_FREE) / 1024 / 1024), 0) size "+
		"FROM information_schema.TABLES WHERE table_schema = '%s' AND TABLE_NAME = '%s'", database, table)

	res, err := db.Query(sqlQuery)
	if err != nil {
		return 0, fmt.Errorf("exec sql query failed, %s", err)
	}

	size := 0
	// 解析mysql结果
	for res.Rows.Next() {
		err = res.Rows.Scan(&size)
		if err != nil {
			return 0, fmt.Errorf("scan rows failed, %s", err)
		}
	}
	err = res.Rows.Close()
	if err != nil {
		return 0, fmt.Errorf("close rows size failed, %s", err)
	}
	return size, err
}

// Explain 获取 SQL 的 explain 信息
func (db *Connect) Explain(sql string) (exp *ExplainInfo, err error) {
	res, err := db.Query(fmt.Sprintf("explain %s", sql))
	if err != nil {
		return exp, err
	}

	// 解析mysql结果，输出ExplainInfo
	exp, err = parseExplainResult(res)
	if err != nil {
		exp.SQL = sql
	}
	return exp, err
}

// parseExplainResult 分析 mysql 执行 explain 的结果，返回 ExplainInfo 结构化数据
func parseExplainResult(res QueryResult) (exp *ExplainInfo, err error) {
	exp = &ExplainInfo{}

	// Different MySQL version has different columns define
	var selectType, table, partitions, accessType, possibleKeys, key, keyLen, ref, extra, rows, filtered []byte
	expRow := ExplainRow{}
	explainFields := make([]interface{}, 0)
	fields := map[string]interface{}{
		"id":            &expRow.ID,
		"select_type":   &selectType,
		"table":         &table,
		"partitions":    &partitions,
		"types":         &accessType,
		"possible_keys": &possibleKeys,
		"key":           &key,
		"key_len":       &keyLen,
		"ref":           &ref,
		"rows":          &rows,
		"filtered":      &filtered,
		"Extra":         &extra,
	}
	cols, err := res.Rows.Columns()
	var colByPass []byte
	for _, col := range cols {
		if _, ok := fields[col]; ok {
			explainFields = append(explainFields, fields[col])
		} else {
			explainFields = append(explainFields, &colByPass)
		}
	}

	// 补全 ExplainRows
	var explainRows []ExplainRow
	for res.Rows.Next() {
		err = res.Rows.Scan(explainFields...)
		if err != nil {
			return nil, err
		}
		expRow.SelectType = NullString(selectType)
		expRow.TableName = NullString(table)
		expRow.Partitions = NullString(partitions)
		expRow.AccessType = NullString(accessType)
		expRow.PossibleKeys = strings.Split(NullString(possibleKeys), ",")
		expRow.Key = NullString(key)
		expRow.KeyLen = NullString(keyLen)
		expRow.Ref = strings.Split(NullString(ref), ",")
		expRow.Rows = NullInt(rows)
		expRow.Filtered = NullFloat(filtered)
		expRow.Extra = NullString(extra)

		// MySQL bug: https://bugs.mysql.com/bug.php?id=34124
		if expRow.Filtered > 100.00 {
			expRow.Filtered = 100.00
		}

		expRow.Scalability = ExplainScalability[expRow.AccessType]
		explainRows = append(explainRows, expRow)
	}
	err = res.Rows.Close()
	if err != nil {
		return nil, err
	}
	exp.ExplainRows = explainRows

	// check explain warning info
	if res.Warning != nil {
		for res.Warning.Next() {
			var expWarning ExplainWarning
			err = res.Warning.Scan(&expWarning.Level, &expWarning.Code, &expWarning.Message)
			if err != nil {
				break
			}

			// 'EXTENDED' is deprecated and will be removed in a future release.
			if expWarning.Code != 1681 {
				exp.Warnings = append(exp.Warnings, expWarning)
			}
		}
		err = res.Warning.Close()
	}

	return exp, err
}

// ExplainScalability ACCESS TYPE对应的运算复杂度 [AccessType]scalability map
var ExplainScalability = map[string]string{
	"NULL":            "NULL",
	"ALL":             "O(n)",
	"index":           "O(n)",
	"range":           "O(log n)+",
	"index_subquery":  "O(log n)+",
	"unique_subquery": "O(log n)+",
	"index_merge":     "O(log n)+",
	"ref_or_null":     "O(log n)+",
	"fulltext":        "O(log n)+",
	"ref":             "O(log n)",
	"eq_ref":          "O(log n)",
	"const":           "O(1)",
	"system":          "O(1)",
}

// NullString null able string
func NullString(buf []byte) string {
	if buf == nil {
		return "NULL"
	}
	return string(buf)
}

// NullFloat null able float
func NullFloat(buf []byte) float64 {
	if buf == nil {
		return 0
	}
	f, _ := strconv.ParseFloat(string(buf), 64)
	return f
}

// NullInt null able int
func NullInt(buf []byte) int64 {
	if buf == nil {
		return 0
	}
	i, _ := strconv.ParseInt(string(buf), 10, 64)
	return i
}

func BuildSelectSQL(table string, columns []string, conditions string) (string, error) {
	col := ""
	for _, s := range columns {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		col += "`" + s + "`" + ","
	}
	if col == "" {
		col = "*"
	} else {
		col = col[:len(col)-1]
	}

	sqlText := fmt.Sprintf("SELECT %s FROM `%s`", col, table)

	conditions = strings.TrimSpace(conditions)
	if conditions != "" {
		sqlText += fmt.Sprintf(" WHERE %s", conditions)
	}

	stmt, err := parser.New().ParseOneStmt(sqlText, "", "")
	if err != nil {
		return "", err
	}

	switch stmt.(type) {
	case *ast.SelectStmt:
		return stmt.Text(), nil
	}
	return "", nil
}
