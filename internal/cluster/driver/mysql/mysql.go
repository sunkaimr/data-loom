package mysql

import (
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/configs"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/pkg/mysql"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"strconv"
	"strings"
)

var ExtractingClusterInfoErr = errors.New("extracting cluster info failed")

type ClusterMysql struct {
	ServiceAddr string
	WriteAddr   string
	ReadAddr    string
	UserName    string
	Password    string
	// 下边是真实使用的密码是解密后的
	host   string
	port   int
	user   string
	passwd string
}

func (c *ClusterMysql) ClusterInfo(ctx *gin.Context) (string, string, string, string, common.ServiceCode, error) {
	_, db := common.ExtractContext(ctx)

	var err error
	c.host, c.port, err = utils.ParseAddr(c.ServiceAddr, "", 3306)
	if err != nil {
		return "", "", "", "", common.CodeClusterServiceAddrErr, fmt.Errorf("parse addr(%s) failed, %s", c.ServiceAddr, err)
	}

	cfg := &models.Config{ID: 1}
	res := db.Model(cfg).Where("id=1").First(cfg)
	if res.Error != nil {
		return "", "", "", "", common.CodeConfigErr, fmt.Errorf("query config from db failed, %s", res.Error)
	}

	// 如果c.UserName为空则以全局为准
	if c.UserName == "" {
		c.UserName = cfg.ClusterDefaultUser
	}
	c.user = c.UserName

	// 如果c.Password为空则以全局为准
	if c.Password == "" {
		c.Password = cfg.ClusterDefaultPasswd
	}

	c.passwd, err = utils.DecryptByAES(c.Password, configs.C.Jwt.Secret)
	if err != nil {
		return "", "", "", "", common.CodeDecryptPasswdErr, fmt.Errorf("decrypt passwd failed, %s", err)
	}

	return c.host, strconv.Itoa(c.port), c.user, c.passwd, common.CodeOK, nil
}

// TestConnect 测试集群是否可达
func (c *ClusterMysql) TestConnect(ctx *gin.Context) (common.ServiceCode, error) {
	log, _ := common.ExtractContext(ctx)

	_, _, _, _, _, err := c.ClusterInfo(ctx)
	if err != nil {
		err = errors.Join(ExtractingClusterInfoErr, err)
		log.Error(err)
		return common.CodeClusterExtractingErr, err
	}

	err = mysql.TestMySQLConnect(c.host, strconv.Itoa(c.port), c.user, c.passwd, "")
	if err != nil {
		err = fmt.Errorf("test connect mysql failed, %s", err)
		log.Error(err)
		return common.CodeClusterUnreachable, err
	}
	return common.CodeOK, nil
}

// GetDatabases 查询库列表
func (c *ClusterMysql) GetDatabases(ctx *gin.Context) ([]string, common.ServiceCode, error) {
	log, _ := common.ExtractContext(ctx)

	_, _, _, _, _, err := c.ClusterInfo(ctx)
	if err != nil {
		err = errors.Join(ExtractingClusterInfoErr, err)
		log.Error(err)
		return nil, common.CodeClusterExtractingErr, err
	}

	con, err := mysql.NewMysqlConnect(c.host, strconv.Itoa(c.port), c.user, c.passwd, "")
	if err != nil {
		err = fmt.Errorf("new mysql connector failed, %s", err)
		log.Error(err)
		return nil, common.CodeConnConnectMysql, err
	}
	defer func() {
		_ = con.Close()
	}()

	var res []string

	res, err = con.ShowDatabases()
	if err != nil {
		err = fmt.Errorf("exec show databases failed, %s", err)
		log.Error(err)
		return nil, common.CodeSourceQueryTableErr, err
	}
	//res = utils.RemoveSubSlices(res, strings.Split(Cfg.ClusterExcludeDatabase, ","))

	return res, common.CodeOK, nil
}

// GetTables 查询表列表
func (c *ClusterMysql) GetTables(ctx *gin.Context, database string) ([]string, common.ServiceCode, error) {
	log, _ := common.ExtractContext(ctx)

	_, _, _, _, _, err := c.ClusterInfo(ctx)
	if err != nil {
		err = errors.Join(ExtractingClusterInfoErr, err)
		log.Error(err)
		return nil, common.CodeClusterExtractingErr, err
	}

	con, err := mysql.NewMysqlConnect(c.host, strconv.Itoa(c.port), c.user, c.passwd, "")
	if err != nil {
		err = fmt.Errorf("new mysql connector failed, %s", err)
		log.Error(err)
		return nil, common.CodeConnConnectMysql, err
	}
	defer func() {
		_ = con.Close()
	}()

	var res []string

	res, err = con.ShowTables(database)
	if err != nil {
		err = fmt.Errorf("exec show databases failed, %s", err)
		log.Error(err)
		return nil, common.CodeSourceQueryDatabaseErr, err
	}
	//res = utils.RemoveSubSlices(res, strings.Split(Cfg.ClusterExcludeTables, ","))

	return res, common.CodeOK, nil
}

func (c *ClusterMysql) TablesHasPrimaryKey(ctx *gin.Context, database string, tables []string) (bool, common.ServiceCode, error) {
	log, _ := common.ExtractContext(ctx)

	_, _, _, _, _, err := c.ClusterInfo(ctx)
	if err != nil {
		err = errors.Join(ExtractingClusterInfoErr, err)
		log.Error(err)
		return false, common.CodeClusterExtractingErr, err
	}

	con, err := mysql.NewMysqlConnect(c.host, strconv.Itoa(c.port), c.user, c.passwd, "")
	if err != nil {
		err = fmt.Errorf("new mysql connector failed, %s", err)
		log.Error(err)
		return false, common.CodeConnConnectMysql, err
	}
	defer func() {
		_ = con.Close()
	}()

	_, err = con.TablesHasPrimaryKey(database, tables)
	if err != nil {
		err = fmt.Errorf("check source table has primary key no pass, %s", err)
		log.Error(err)
		return false, common.CodeSourceTableHasPrimaryKey, err
	}

	return true, common.CodeOK, nil
}

func (c *ClusterMysql) SQLExplain(ctx *gin.Context, database, table, columns, conditions string) (int64, common.ServiceCode, error) {
	log, _ := common.ExtractContext(ctx)

	_, _, _, _, _, err := c.ClusterInfo(ctx)
	if err != nil {
		err = errors.Join(ExtractingClusterInfoErr, err)
		log.Error(err)
		return 0, common.CodeClusterExtractingErr, err
	}

	con, err := mysql.NewMysqlConnect(c.host, strconv.Itoa(c.port), c.user, c.passwd, database)
	if err != nil {
		err = fmt.Errorf("new mysql connector failed, %s", err)
		log.Error(err)
		return 0, common.CodeConnConnectMysql, err
	}
	defer func() {
		_ = con.Close()
	}()

	sql, err := mysql.BuildSelectSQL(table, strings.Split(columns, ","), conditions)
	if err != nil {
		err = fmt.Errorf("build select sql failed, %s", err)
		log.Error(err)
		return 0, common.CodePolicyConditionsErr, err
	}

	explain, err := con.Explain(sql)
	if err != nil {
		err = fmt.Errorf("explain %s failed, %s", sql, err)
		log.Error(err)
		return 0, common.CodePolicyConditionsErr, err
	}

	var affectRows int64
	for _, v := range explain.ExplainRows {
		if v.Rows > affectRows {
			affectRows = v.Rows
		}
	}
	return affectRows, common.CodeOK, nil
}

func (c *ClusterMysql) GetTablesSize(ctx *gin.Context, database string, tables []string) (int, common.ServiceCode, error) {
	log, _ := common.ExtractContext(ctx)

	_, _, _, _, _, err := c.ClusterInfo(ctx)
	if err != nil {
		err = errors.Join(ExtractingClusterInfoErr, err)
		log.Error(err)
		return 0, common.CodeClusterExtractingErr, err
	}

	con, err := mysql.NewMysqlConnect(c.host, strconv.Itoa(c.port), c.user, c.passwd, "")
	if err != nil {
		err = fmt.Errorf("new mysql connector failed, %s", err)
		log.Error(err)
		return 0, common.CodeConnConnectMysql, err
	}
	defer func() {
		_ = con.Close()
	}()

	total, size := 0, 0
	for _, table := range tables {
		size, err = con.TableSize(database, table)
		if err != nil {
			err = fmt.Errorf("get table(%s) size failed, %s", table, err)
			log.Error(err)
			return 0, common.CodeSourceQueryTableSizeErr, err
		}
		total += size
	}

	return total, common.CodeOK, nil
}

// GetClusterBigTables 获取集群的大表信息
// 返回interface类型使用类型转换为如下格式使用
//
//	struct {
//		Database string
//		Table    string
//		Size     int
//	}
func (c *ClusterMysql) GetClusterBigTables(ctx *gin.Context, threshold int) (any, common.ServiceCode, error) {
	log, _ := common.ExtractContext(ctx)

	_, _, _, _, _, err := c.ClusterInfo(ctx)
	if err != nil {
		err = errors.Join(ExtractingClusterInfoErr, err)
		log.Error(err)
		return nil, common.CodeClusterExtractingErr, err
	}

	con, err := mysql.NewMysqlConnect(c.host, strconv.Itoa(c.port), c.user, c.passwd, "")
	if err != nil {
		err = fmt.Errorf("new mysql connector failed, %s", err)
		log.Error(err)
		return nil, common.CodeConnConnectMysql, err
	}
	defer func() {
		_ = con.Close()
	}()

	res, err := con.Query(
		fmt.Sprintf(
			"SELECT "+
				"  TABLE_SCHEMA, TABLE_NAME, ROUND((DATA_LENGTH + INDEX_LENGTH + DATA_FREE) / 1024 / 1024 / 1024, 0) AS size "+
				"FROM "+
				"  information_schema.TABLES WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')"+
				"  AND (DATA_LENGTH + INDEX_LENGTH + DATA_FREE) / 1024 / 1024 / 1024 >= %d", threshold))
	if err != nil {
		return nil, common.CodeServerErr, fmt.Errorf("exec sql query failed, %s", err)
	}

	var rows []any
	for res.Rows.Next() {
		d, t, size := "", "", 0
		err = res.Rows.Scan(&d, &t, &size)
		if err != nil {
			return nil, common.CodeServerErr, fmt.Errorf("scan rows failed, %s", err)
		}
		rows = append(rows, struct {
			Database string
			Table    string
			Size     int
		}{
			Database: d,
			Table:    t,
			Size:     size,
		})
	}

	err = res.Rows.Close()
	if err != nil {
		return rows, common.CodeServerErr, fmt.Errorf("close scan rows failed, %s", err)
	}

	return rows, common.CodeOK, nil
}

func (c *ClusterMysql) GetFreeDisk(ctx *gin.Context) (int, error) {
	//_, db := common.ExtractContext(ctx)
	//
	//cfg := &models.Config{ID: 1}
	//res := db.Model(cfg).Where("id=1").First(cfg)
	//if res.Error != nil {
	//	return 0, fmt.Errorf("query cfg from db failed, %s", res.Error)
	//}
	//
	//ip, _, _ := utils.ParseAddr(c.ServiceAddr, "", 3306)
	//disk, err := prometheus.NewClient(cfg.ThanosUrl).DiskFree(ip, time.Now())
	//if err == nil {
	//	return int(disk), nil
	//}
	//
	//if !errors.Is(err, prometheus.NoDataPointError) {
	//	return int(disk), err
	//}
	//
	//// 找不到数据，向前提5min查读写库的空间
	//ip, _, _ = utils.ParseAddr(c.WriteAddr, "", 3306)
	//disk, err = prometheus.NewClient(cfg.ThanosUrl).DiskFree(ip, time.Now().Add(-5*time.Minute))
	//return int(disk), err
	return 0, nil
}

func (c *ClusterMysql) GetDiskUsage(ctx *gin.Context) (int, error) {
	//_, db := common.ExtractContext(ctx)
	//
	//cfg := &models.Config{ID: 1}
	//res := db.Model(cfg).Where("id=1").First(cfg)
	//if res.Error != nil {
	//	return 0, fmt.Errorf("query cfg from db failed, %s", res.Error)
	//}
	//
	//ip, _, _ := utils.ParseAddr(c.ServiceAddr, "", 3306)
	//disk, err := prometheus.NewClient(cfg.ThanosUrl).DiskUsage(ip, time.Now())
	//if err == nil {
	//	return int(disk), nil
	//}
	//
	//if !errors.Is(err, prometheus.NoDataPointError) {
	//	return int(disk), err
	//}
	//
	//// 找不到数据，向前提5min查读写库的空间
	//ip, _, _ = utils.ParseAddr(c.WriteAddr, "", 3306)
	//disk, err = prometheus.NewClient(cfg.ThanosUrl).DiskUsage(ip, time.Now().Add(-5*time.Minute))
	//return int(disk), err
	return 0, nil
}

func (c *ClusterMysql) GetDiskUsed(ctx *gin.Context) (int, error) {
	//_, db := common.ExtractContext(ctx)
	//
	//cfg := &models.Config{ID: 1}
	//res := db.Model(cfg).Where("id=1").First(cfg)
	//if res.Error != nil {
	//	return 0, fmt.Errorf("query cfg from db failed, %s", res.Error)
	//}
	//
	//ip, _, _ := utils.ParseAddr(c.ServiceAddr, "", 3306)
	//disk, err := prometheus.NewClient(cfg.ThanosUrl).DiskUsed(ip, time.Now())
	//if err == nil {
	//	return int(disk), nil
	//}
	//
	//if !errors.Is(err, prometheus.NoDataPointError) {
	//	return int(disk), err
	//}
	//
	//// 找不到数据，向前提5min查读写库的空间
	//ip, _, _ = utils.ParseAddr(c.WriteAddr, "", 3306)
	//disk, err = prometheus.NewClient(cfg.ThanosUrl).DiskUsed(ip, time.Now().Add(-5*time.Minute))
	//return int(disk), err
	return 0, nil
}
