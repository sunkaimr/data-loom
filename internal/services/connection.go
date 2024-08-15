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
	"time"
)

type ConnService struct {
	Model
	Name         string             `json:"name"`           // 连接名称
	Description  string             `json:"description"`    // 说明
	Bu           string             `json:"bu"`             // 资产BU
	Storage      common.StorageType `json:"storage"`        // 归档介质
	DataBendAddr string             `json:"data_bend_addr"` // DataBend的AK
	DataBendAK   string             `json:"data_bend_ak"`   // DataBend的SK
	DataBendSK   string             `json:"data_bend_sk"`   // DataBend地址
	MysqlHost    string             `json:"mysql_host"`     // mysql地址
	MysqlPort    string             `json:"mysql_port"`     // mysql端口
	MysqlUser    string             `json:"mysql_user"`     // mysql用户名
	MysqlPasswd  string             `json:"mysql_passwd"`   // mysql密码
}

func (c *ConnService) CheckParameters(_ *gin.Context) (bool, common.ServiceCode, error) {
	// 检验源端名字长度
	if len(c.Name) == 0 || len(c.Name) == 1024 {
		return false, common.CodeConnNameEmpty, fmt.Errorf("validate conn name(%s) not pass", c.Name)
	}

	if len(c.Bu) <= 0 || len(c.Bu) >= 64 {
		return false, common.CodeConnBuNameEmpty, fmt.Errorf("validate conn bu(%s) not pass", c.Bu)
	}

	switch c.Storage {
	case common.StorageMysql:
		// 校验mysql连接信息
		err := mysql.TestMySQLConnect(c.MysqlHost, c.MysqlPort, c.MysqlUser, c.MysqlPasswd, "")
		if err != nil {
			return false, common.CodeConnTestErr, fmt.Errorf("test mysql connection not pass, %s", err)
		}
	case common.StorageDataBend:
		// TODO 校验DataBend连接信息
	default:
		return false, common.CodeConnStorageErr, fmt.Errorf("validate storage(%s) not pass", c.Storage)
	}

	return true, common.CodeOK, nil
}

func (c *ConnService) CreateConn(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	conn := c.ServiceToModel()
	conn.CreatedAt = time.Now()
	conn.Creator = u.UserName

	var count int64
	err := db.Model(conn).Where("name =?", conn.Name).Count(&count).Error
	if err != nil {
		log.Errorf("query models.Connection(name=%s) from db failed, %s", conn.Name, err)
		return common.CodeServerErr, err
	}
	if count != 0 {
		err = fmt.Errorf("models.Connection(name=%s) exist", conn.Name)
		return common.CodeConnExist, err
	}

	switch common.StorageType(conn.Storage) {
	case common.StorageMysql:
		conn.MysqlPasswd, err = utils.EncryptByAES(conn.MysqlPasswd, configs.C.Jwt.Secret)
		if err != nil {
			log.Errorf("encrypt models.Connection(%v) failed, %s", c.ID, err)
			return common.CodeEncryptPasswdErr, err
		}
	}

	err = db.Save(conn).Error
	if err != nil {
		log.Errorf("save models.Connection(%+v) to db failed, %s", conn, err)
		return common.CodeServerErr, err
	}

	c.ModelToService(conn)

	return common.CodeOK, nil
}

func (c *ConnService) UpdateConn(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	conn := &models.Connection{}
	err := db.Model(&conn).First(conn, "id = ?", c.ID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Errorf("query models.Connection(id=%d) not exist", conn.ID)
			return common.CodeConnNotExist, err
		}
		log.Errorf("query models.Connection(id=%d) from db failed, %s", conn.ID, err)
		return common.CodeServerErr, err
	}

	var count int64
	err = db.Model(conn).Where("name =? AND id !=?", c.Name, c.ID).Count(&count).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Errorf("query models.Connection(name =%v AND id !=%v) from db failed, %s", c.Name, c.ID, err)
		return common.CodeServerErr, err
	}
	if count != 0 {
		err = fmt.Errorf("models.Connection(name=%s) exist", c.Name)
		log.Error(err)
		return common.CodeConnNameConflict, err
	}

	if c.Storage != "" && conn.Storage != c.Storage {
		err = fmt.Errorf("models.Connection(Storage=%s) is unchangeable", conn.Storage)
		log.Error(err)
		return common.CodeConnStorageImmutable, err
	}

	conn.Name = c.Name
	conn.Description = c.Description
	conn.Bu = c.Bu
	conn.Editor = u.UserName
	conn.MysqlHost = utils.Ternary[string](c.MysqlHost == "", conn.MysqlHost, c.MysqlHost)
	conn.MysqlPort = utils.Ternary[string](c.MysqlPort == "", conn.MysqlPort, c.MysqlPort)
	conn.MysqlUser = utils.Ternary[string](c.MysqlUser == "", conn.MysqlUser, c.MysqlUser)
	conn.DataBendAddr = utils.Ternary[string](c.DataBendAddr == "", conn.DataBendAddr, c.DataBendAddr)
	conn.DataBendAK = utils.Ternary[string](c.DataBendAK == "", conn.DataBendAK, c.DataBendAK)
	conn.DataBendSK = utils.Ternary[string](c.DataBendSK == "", conn.DataBendSK, c.DataBendSK)

	switch conn.Storage {
	case common.StorageMysql:
		// 用户没有传入passwd，则以库里为准，需要先解密再校验
		if c.MysqlPasswd == "" {
			conn.MysqlPasswd, err = utils.DecryptByAES(conn.MysqlPasswd, configs.C.Jwt.Secret)
			if err != nil {
				log.Errorf("decrypt models.Connection(%v) MysqlPasswd failed, %s", c.ID, err)
				return common.CodeDecryptPasswdErr, err
			}
		} else {
			// 用户传递了password，则可能是加密过的，也可能是未加密的
			// 尝试去解密，如果解密失败，则认为用户传递的是未加密的
			conn.MysqlPasswd, err = utils.DecryptByAES(c.MysqlPasswd, configs.C.Jwt.Secret)
			if err != nil {
				log.Infof("decrypt user update MysqlPasswd failed, it may be raw password %s", err)
				conn.MysqlPasswd = c.MysqlPasswd
			}
		}
	}

	c.ModelToService(conn)

	// 参数校验
	if ok, res, err := c.CheckParameters(ctx); !ok {
		log.Errorf("check parameters(%+v) not pass, %s", c, err)
		return res, err
	}

	// 入库之前先加密密码
	switch conn.Storage {
	case common.StorageMysql:
		conn.MysqlPasswd, err = utils.EncryptByAES(conn.MysqlPasswd, configs.C.Jwt.Secret)
		if err != nil {
			log.Errorf("encrypt models.Connection(%v) failed, %s", c.ID, err)
			return common.CodeEncryptPasswdErr, err
		}
	}

	err = db.Save(conn).Error
	if err != nil {
		log.Errorf("update models.Connection(%+v) from db failed, %s", conn, err)
		return common.CodeServerErr, err
	}

	c.ModelToService(conn)

	return common.CodeOK, nil
}

func (c *ConnService) DeleteConn(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)
	var err error

	conn := models.Connection{}

	// 若ID有效则以ID为准, 否则以name为准
	if !common.InvalidUintID(c.ID) {
		err = db.Model(&conn).First(&conn, "id = ?", c.ID).Error
	} else {
		err = db.Model(&conn).First(&conn, "name = ?", c.Name).Error
	}
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.Connection(%v|%v) not exist", c.ID, c.Name)
			log.Error(err)
			return common.CodeConnNotExist, err
		}
		err = fmt.Errorf("query models.Connection(%v|%v) from db failed, %s", c.ID, c.Name, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	// 校验源端是否被策略引用，如果引用不可删除
	var dest []models.Destination
	err = db.Model(&models.Destination{}).Select("name").Where("connection_id =?", conn.ID).Find(&dest).Error
	if err != nil {
		err = fmt.Errorf("query models.Destination(connect=%d) from db failed, %s", conn.ID, err)
		log.Error(err)
		return common.CodeServerErr, err
	}
	if len(dest) != 0 {
		err = fmt.Errorf("connect(%s) has been used form Destination(%s)", conn.Name, dest[0].Name)
		log.Error(err)
		return common.CodeConnUsingDest, err
	}

	// 未完成转态的任务会使用到dest_connection_id，不可删除
	var task []models.Task
	err = db.Model(&models.Task{}).Select("name").Where("dest_connection_id =? AND task_status IN (?)",
		conn.ID, common.TaskStatusNotFinish).
		Find(&task).Error
	if err != nil {
		err = fmt.Errorf("query models.Destination(dest_connection_id =%v AND task_status IN (%v) from db failed, %s",
			conn.ID, common.TaskStatusNotFinish, err)
		log.Error(err)
		return common.CodeServerErr, err
	}
	if len(task) != 0 {
		err = fmt.Errorf("connect(%s) has been used form Task(%s)", conn.Name, task[0].Name)
		log.Error(err)
		return common.CodeConnUsingTask, err
	}

	db.Model(&models.Connection{}).Where("id =?", conn.ID).Update("editor", u.UserName)
	err = db.Delete(&models.Connection{}, "id =?", conn.ID).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		err = fmt.Errorf("delete models.Connection(name=%s) from db failed, %s", conn.Name, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	return common.CodeOK, nil
}

func (c *ConnService) QueryConn(ctx *gin.Context, queryMap map[string]string) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	res, err := common.NewPageList[[]models.Connection](db).
		QueryPaging(ctx).
		Order("id desc").
		Query(
			common.FilterFuzzyStringMap(queryMap),
			common.FilterID(c.ID),
		)
	if err != nil {
		err = fmt.Errorf("query models.Connection from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}
	return res, common.CodeOK, nil
}

// TestConn 测试连接信息是否正确（校验地址和用户名密码）
func (c *ConnService) TestConn(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	conn := models.Connection{}

	// 若ID有效则以ID为准, 否则以用户提供为准
	if !common.InvalidUintID(c.ID) {
		err := db.Model(&conn).First(&conn, "id = ?", c.ID).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				log.Infof("query models.Connection(%v) not exist", c.ID)
				return common.CodeConnNotExist, err
			}
			log.Errorf("query models.Connection(%v) from db failed, %s", c.ID, err)
			return common.CodeServerErr, err
		}

		// 以ID为准覆盖用户提供的信息
		switch conn.Storage {
		case common.StorageMysql:
			conn.MysqlPasswd, err = utils.DecryptByAES(conn.MysqlPasswd, configs.C.Jwt.Secret)
			if err != nil {
				log.Errorf("decrypt models.Connection(%v) failed, %s", c.ID, err)
				return common.CodeDecryptPasswdErr, err
			}
		}
		c.ModelToService(&conn)
	}

	if ok, res, err := c.CheckParameters(ctx); !ok {
		log.Errorf("check parameters(%+v) not pass, %s", c, err)
		return res, err
	}
	return common.CodeConnTestOK, nil
}

func (c *ConnService) ServiceToModel() *models.Connection {
	m := &models.Connection{}
	m.ID = c.ID
	m.Creator = c.Creator
	m.Editor = c.Editor
	m.CreatedAt, _ = time.ParseInLocation(time.DateTime, c.CreatedAt, time.Now().Location())
	m.UpdatedAt, _ = time.ParseInLocation(time.DateTime, c.UpdatedAt, time.Now().Location())
	m.Name = c.Name
	m.Description = c.Description
	m.Bu = c.Bu
	m.Storage = c.Storage
	m.DataBendAK = c.DataBendAK
	m.DataBendSK = c.DataBendSK
	m.DataBendAddr = c.DataBendAddr
	m.MysqlHost = c.MysqlHost
	m.MysqlPort = c.MysqlPort
	m.MysqlUser = c.MysqlUser
	m.MysqlPasswd = c.MysqlPasswd
	return m
}

func (c *ConnService) ModelToService(m *models.Connection) *ConnService {
	c.ID = m.ID
	c.Creator = m.Creator
	c.Editor = m.Editor
	c.CreatedAt = m.CreatedAt.Format(time.DateTime)
	c.UpdatedAt = m.UpdatedAt.Format(time.DateTime)
	c.Name = m.Name
	c.Description = m.Description
	c.Bu = m.Bu
	c.Storage = m.Storage
	c.DataBendAK = m.DataBendAK
	c.DataBendSK = m.DataBendSK
	c.DataBendAddr = m.DataBendAddr
	c.MysqlHost = m.MysqlHost
	c.MysqlPort = m.MysqlPort
	c.MysqlUser = m.MysqlUser
	c.MysqlPasswd = m.MysqlPasswd
	return c
}
