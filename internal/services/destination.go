package services

import (
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/configs"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/pkg/mysql"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"gorm.io/gorm"
	"regexp"
	"strings"
	"time"
)

type DestService struct {
	Model
	Name         string             `json:"name"`                                                                            // 目标端名称
	Description  string             `json:"description"`                                                                     // 说明
	Storage      common.StorageType `json:"storage"`                                                                         // 归档介质
	ConnectionID uint               `json:"connection_id"`                                                                   // 归档库连接信息
	DatabaseName string             `json:"database_name"`                                                                   // 归档库名
	TableName    string             `json:"table_name" example:"{source_table},table123_{YYYY-MM},{source_table}_{YYYY-MM}"` // 归档表名
	Compress     bool               `json:"compress"`                                                                        // 是否压缩存储
}

func (c *DestService) CheckParameters(ctx *gin.Context) (bool, common.ServiceCode, error) {
	_, db := common.ExtractContext(ctx)
	// 检验源端名字长度
	if len(c.Name) == 0 || len(c.Name) == 1024 {
		return false, common.CodeDestNameEmpty, fmt.Errorf("validate conn name(%s) not pass", c.Name)
	}

	conn := &models.Connection{}
	err := db.Model(conn).Where("id =?", c.ConnectionID).First(conn).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, common.CodeConnNotExist, fmt.Errorf("models.Connection(%v) not exist", c.ConnectionID)
		}
		return false, common.CodeServerErr, fmt.Errorf("query models.Connection(%v) failed, %s", c.ConnectionID, err)
	}

	if c.Storage != conn.Storage {
		return false, common.CodeDestStorageNotMatch, fmt.Errorf("dest.Storage(%s) must equel conn..Storage(%s)", c.Storage, conn.Storage)
	}

	if c.Storage != common.StorageMysql && c.Compress {
		return false, common.CodeDestCompressOnlyMysql, fmt.Errorf("compress only can use for storage mysql")
	}

	connSvc := &ConnService{}
	connSvc.ModelToService(conn)
	connSvc.MysqlPasswd, err = utils.DecryptByAES(connSvc.MysqlPasswd, configs.C.Jwt.Secret)
	if err != nil {
		err = fmt.Errorf("decrypt passwd failed, %s", err)
		return false, common.CodeDecryptPasswdErr, err
	}
	if ok, res, err := connSvc.CheckParameters(ctx); !ok {
		err = fmt.Errorf("check connect(%v) not pass, %s", conn.Name, err)
		return false, res, err
	}

	// c.Database：目标端库非必填，若目标库名称为空默认和源端保持一致，目标库不存在会自动创建目标库

	// c.Table 目标表必填字段可以为：
	// 1，自定义名字：自定义表名每次归档都向该表写入
	// 2，和源端保持一致格式如{source_table}：和源端表名字保持一致，每次归档都向该表写入
	// 3，以日期做为后缀格式如{source_table}_{YYYY-MM}或table123_{YYYY-MM}：每次归档以当前的日期创建新表
	if !validateDestTableName(c.TableName) {
		return false, common.CodeDestTableNameErr, fmt.Errorf("validate table name(%s) not pass", c.TableName)
	}

	// TODO 在创建策略时如果目标库、表都存在时需要提示用户自己确实是否要向目标表归档

	return true, common.CodeOK, nil
}

func (c *DestService) CreateDest(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	dest := c.ServiceToModel()
	dest.Creator = u.UserName

	var count int64
	err := db.Model(dest).Where("name =?", dest.Name).Count(&count).Error
	if err != nil {
		log.Errorf("query models.Destination(name=%s) from db failed, %s", dest.Name, err)
		return common.CodeServerErr, err
	}
	if count != 0 {
		err = fmt.Errorf("models.Destination(name=%s) exist", dest.Name)
		return common.CodeDestExist, err
	}

	err = db.Save(dest).Error
	if err != nil {
		log.Errorf("save models.Destination(%+v) to db failed, %s", dest, err)
		return common.CodeServerErr, err
	}

	c.ModelToService(dest)

	return common.CodeOK, nil
}

func (c *DestService) CheckUpdateParameters(ctx *gin.Context) (bool, common.ServiceCode, error) {
	var err error
	log, db := common.ExtractContext(ctx)

	if common.InvalidUintID(c.ID) {
		return false, common.CodeInvalidID, fmt.Errorf("invalid Destination.id(%d)", c.ID)
	}

	if len(c.Name) == 0 || len(c.Name) == 1024 {
		err = fmt.Errorf("invalid Destination.name(%v)", c.Name)
		log.Error(err)
		return false, common.CodeDestNameLenErr, err
	}

	dest := &models.Destination{}
	err = db.Model(&dest).First(dest, "id = ?", c.ID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.Destination(id=%d) not exist", dest.ID)
			log.Error(err)
			return false, common.CodeDestNotExist, err
		}
		err = fmt.Errorf("query models.Destination(id=%d) from db failed, %s", dest.ID, err)
		log.Error(err)
		return false, common.CodeServerErr, err
	}

	if c.Storage != "" && c.Storage != dest.Storage {
		err = fmt.Errorf("request.Destination.Storage(%v) != models.Destination.Storage(%v)", c.Storage, dest.Storage)
		log.Error(err)
		return false, common.CodeDestStorageImmutable, err
	}

	if !common.InvalidUintID(c.ConnectionID) && c.ConnectionID != dest.ConnectionID {
		err = fmt.Errorf("request.Destination.ConnectionID(%v) != models.Destination.ConnectionID(%v)", c.ConnectionID, dest.ConnectionID)
		log.Error(err)
		return false, common.CodeDestConnectionIDImmutable, err
	}

	if c.DatabaseName != dest.DatabaseName {
		err = fmt.Errorf("request.Destination.DatabaseName(%v) != models.Destination.DatabaseName(%v)", c.DatabaseName, dest.DatabaseName)
		log.Error(err)
		return false, common.CodeDestDBImmutable, err
	}

	if c.TableName != dest.TableName {
		err = fmt.Errorf("request.Destination.TableName(%v) != models.Destination.TableName(%v)", c.TableName, dest.TableName)
		log.Error(err)
		return false, common.CodeDestTableImmutable, err
	}

	if c.Compress != dest.Compress {
		err = fmt.Errorf("request.Destination.Compress(%v) != models.Destination.Compress(%v)", c.Compress, dest.Compress)
		log.Error(err)
		return false, common.CodeDestCompressImmutable, err
	}

	return true, common.CodeOK, nil
}

func (c *DestService) UpdateDest(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	dest := &models.Destination{}
	err := db.Model(&dest).First(dest, "id = ?", c.ID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.Destination(id=%d) not exist", dest.ID)
			log.Error(err)
			return common.CodeDestNotExist, err
		}
		err = fmt.Errorf("query models.Destination(id=%d) from db failed, %s", dest.ID, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	var count int64
	err = db.Model(dest).Where("name =? AND id !=?", c.Name, c.ID).Count(&count).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Errorf("query models.Destination(name =%v AND id !=%v) from db failed, %s", c.Name, c.ID, err)
		return common.CodeServerErr, err
	}
	if count != 0 {
		err = fmt.Errorf("models.Destination(name=%s) exist", c.Name)
		log.Error(err)
		return common.CodeDestNameConflict, err
	}

	dest.Name = c.Name
	dest.Description = c.Description
	dest.Editor = u.UserName
	err = db.Save(dest).Error
	if err != nil {
		log.Errorf("update models.Destination(%+v) from db failed, %s", dest, err)
		return common.CodeServerErr, err
	}

	c.ModelToService(dest)

	return common.CodeOK, nil
}

func (c *DestService) DeleteDest(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)
	var err error

	dest := models.Destination{}

	// 若ID有效则以ID为准, 否则以name为准
	if !common.InvalidUintID(c.ID) {
		err = db.Model(&dest).First(&dest, "id = ?", c.ID).Error
	} else {
		err = db.Model(&dest).First(&dest, "name = ?", c.Name).Error
	}
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Infof("query models.Destination(%v|%v) not exist", c.ID, c.Name)
			return common.CodeDestNotExist, err
		}
		err = fmt.Errorf("query models.Destination(%v|%v) from db failed, %s", c.ID, c.Name, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	// 校验是否被策略引用，如果引用不可删除
	var policies []models.Policy
	err = db.Model(&models.Policy{}).Select("name").Where("dest_id =?", dest.ID).Find(&policies).Error
	if err != nil {
		err = fmt.Errorf("query models.Destination(dest_id=%v) from db failed, %s", dest.ID, err)
		log.Error(err)
		return common.CodeServerErr, err
	}
	if len(policies) != 0 {
		err = fmt.Errorf("destination(%s) has been used form Policy(%s)", dest.Name, policies[0].Name)
		log.Error(err)
		return common.CodeDestUsingPolicy, err
	}

	// 未完成转态的任务会使用到，不可删除
	var task []models.Task
	err = db.Model(&models.Task{}).Select("name").Where("dest_id =? AND task_status IN (?)", c.ID, common.TaskStatusNotFinish).Find(&task).Error
	if err != nil {
		err = fmt.Errorf("query models.Task(dest_id=%v AND task_status IN (%v)) from db failed, %s", c.ID, common.TaskStatusNotFinish, err)
		log.Error(err)
		return common.CodeServerErr, err
	}
	if len(task) != 0 {
		err = fmt.Errorf("dest(%s) has been used form Task(%s)", c.Name, task[0].Name)
		log.Error(err)
		return common.CodeDestUsingTask, err
	}

	db.Model(&models.Destination{}).Where("id =?", dest.ID).Update("editor", u.UserName)
	err = db.Delete(&models.Destination{}, "id =?", dest.ID).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		err = fmt.Errorf("delete models.Destination(name=%s) from db failed, %s", dest.Name, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	return common.CodeOK, nil
}

func (c *DestService) QueryDest(ctx *gin.Context, queryMap map[string]string) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	compress, compressOk := ctx.GetQuery("compress")
	res, err := common.NewPageList[[]models.Destination](db).
		QueryPaging(ctx).
		Order("id desc").
		Query(
			common.FilterFuzzyStringMap(queryMap),
			common.FilterID(c.ID),
			common.FilterCustomUintID("connection_id", c.ConnectionID),
			common.FilterCustomBool("compress", compress, compressOk),
		)
	if err != nil {
		err = fmt.Errorf("query models.Destination from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}
	return res, common.CodeOK, nil
}

// validateDestTableName 检查 Table 是否符合指定的格式
//
//	1，自定义名字：自定义表名每次归档都向该表写入
//	2，和源端保持一致格式如{source_table}：和源端表名字保持一致，每次归档都向该表写入
//	3，以日期做为后缀格式如{source_table}_{YYYY-MM}或table123_{YYYY-MM}：每次归档以当前的日期创建新表
func validateDestTableName(name string) bool {
	// 1. 自定义名字：无特殊格式要求，简单检查非空
	if name != "" && !strings.Contains(name, "{") {
		return true
	}

	// 2. 和源端保持一致格式如{source_table}
	if regexp.MustCompile(`^{source_table}$`).MatchString(name) {
		return true
	}

	// 3. 以日期做为后缀格式如{source_table}_{YYYY-MM}或table123_{YYYY-MM}
	reDateSuffix := regexp.MustCompile(`(?P<prefix>.*)_{YYYY-MM}$`)
	if match := reDateSuffix.FindStringSubmatch(name); match != nil {
		prefix := match[reDateSuffix.SubexpIndex("prefix")]
		if prefix == "" {
			return false
		}
		return true
	}

	return false
}

func (c *DestService) DestDatabaseExist(ctx *gin.Context) (bool, common.ServiceCode, error) {
	_, db := common.ExtractContext(ctx)

	conn := &models.Connection{}
	err := db.Model(conn).Where("id =?", c.ConnectionID).First(conn).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, common.CodeConnNotExist, fmt.Errorf("models.Connection(%v) not exist", c.ConnectionID)
		}
		return false, common.CodeServerErr, fmt.Errorf("query models.Connection(%v) failed, %s", c.ConnectionID, err)
	}

	switch c.Storage {
	case common.StorageMysql:
		mysqlConn, err := mysql.NewMysqlConnect(conn.MysqlHost, conn.MysqlPort, conn.MysqlUser, conn.MysqlPasswd, c.DatabaseName)
		if err != nil {
			return false, common.CodeConnConnectMysql, fmt.Errorf("new mysql connect failed, %s", err)
		}
		defer func() { _ = mysqlConn.Close() }()

		databases, err := mysqlConn.ShowDatabases()
		if err != nil {
			return false, common.CodeDestQueryDatabaseErr, fmt.Errorf("show databases failed, %s", err)
		}
		if utils.ElementExist(c.DatabaseName, databases) {
			return true, common.CodeDestDatabaseExist, nil
		} else {
			return false, common.CodeDestDatabaseNotExist, nil
		}
	case common.StorageDataBend:
		// TODO 查询DataBend库信息
	default:
		return false, common.CodeConnStorageErr, fmt.Errorf("validate storage(%s) not pass", c.Storage)
	}

	// 正常不应该运行到此处
	return false, common.CodeServerErr, nil
}

func (c *DestService) DestTableExist(ctx *gin.Context) (bool, common.ServiceCode, error) {
	_, db := common.ExtractContext(ctx)

	conn := &models.Connection{}
	err := db.Model(conn).Where("id =?", c.ConnectionID).First(conn).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, common.CodeConnNotExist, fmt.Errorf("models.Connection(%v) not exist", c.ConnectionID)
		}
		return false, common.CodeServerErr, fmt.Errorf("query models.Connection(%v) failed, %s", c.ConnectionID, err)
	}

	switch c.Storage {
	case common.StorageMysql:
		// 校验mysql连接信息
		mysqlConn, err := mysql.NewMysqlConnect(conn.MysqlHost, conn.MysqlPort, conn.MysqlUser, conn.MysqlPasswd, c.DatabaseName)
		if err != nil {
			return false, common.CodeConnConnectMysql, fmt.Errorf("new mysql connect failed, %s", err)
		}
		defer func() { _ = mysqlConn.Close() }()

		tables, err := mysqlConn.ShowTables(c.DatabaseName)
		if err != nil {
			return false, common.CodeDestQueryTableErr, fmt.Errorf("show tables failed, %s", err)
		}
		if utils.ElementExist(c.DatabaseName, tables) {
			return true, common.CodeDestTablesExist, nil
		} else {
			return false, common.CodeDestTableNotExist, nil
		}
	case common.StorageDataBend:
		// TODO 查询DataBend的表信息
	default:
		return false, common.CodeConnStorageErr, fmt.Errorf("validate storage(%s) not pass", c.Storage)
	}

	// 正常不应该运行到此处
	return false, common.CodeServerErr, nil
}

func (c *DestService) ServiceToModel() *models.Destination {
	m := &models.Destination{}
	m.ID = c.ID
	m.Creator = c.Creator
	m.Editor = c.Editor
	m.CreatedAt, _ = time.ParseInLocation(time.DateTime, c.CreatedAt, time.Now().Location())
	m.UpdatedAt, _ = time.ParseInLocation(time.DateTime, c.UpdatedAt, time.Now().Location())
	m.Name = c.Name
	m.Description = c.Description
	m.Storage = c.Storage
	m.ConnectionID = c.ConnectionID
	m.DatabaseName = c.DatabaseName
	m.TableName = c.TableName
	m.Compress = c.Compress
	return m
}

func (c *DestService) ModelToService(m *models.Destination) *DestService {
	c.ID = m.ID
	c.Creator = m.Creator
	c.Editor = m.Editor
	c.CreatedAt = m.CreatedAt.Format(time.DateTime)
	c.UpdatedAt = m.UpdatedAt.Format(time.DateTime)
	c.Name = m.Name
	c.Description = m.Description
	c.Storage = m.Storage
	c.ConnectionID = m.ConnectionID
	c.DatabaseName = m.DatabaseName
	c.TableName = m.TableName
	c.Compress = m.Compress
	return c
}
