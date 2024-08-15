package models

import (
	"database/sql/driver"
	"errors"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	l "github.com/sunkaimr/data-loom/internal/pkg/logger"
	"github.com/sunkaimr/data-loom/internal/pkg/mysql"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"gorm.io/gorm"
	"time"
)

type Model struct {
	ID        uint           `json:"id" gorm:"primary_key;AUTO_INCREMENT;comment:ID"`
	CreatedAt time.Time      `json:"created_at" gorm:"type:DATETIME;comment:创建时间"`
	UpdatedAt time.Time      `json:"updated_at" gorm:"type:DATETIME;comment:修改时间"`
	DeletedAt gorm.DeletedAt `json:"deleted_at,omitempty" gorm:"type:DATETIME;index;comment:删除时间"`
	Creator   string         `json:"creator" gorm:"type:varchar(64);not null;comment:创建人"`
	Editor    string         `json:"editor" gorm:"type:varchar(64);comment:修改人"`
}

func UpdateModels() {
	err := mysql.DB.AutoMigrate(
		&User{},
		&Connection{},
		&Destination{},
		&Policy{},
		&PolicyRevision{},
		&TaskRevision{},
		&Source{},
		&Cluster{},
		&ClusterStatistics{},
		&Task{},
		&TaskChangeLog{},
		&Config{},
	)
	if err != nil {
		l.Log.Error(err)
	}

	var count int64
	err = mysql.DB.Model(User{}).Where("username =?", common.AdminUser).Count(&count).Error
	if err == nil && count == 0 {
		admin := new(User)
		admin.Username = common.AdminUser
		admin.RealName = common.AdminName
		admin.Password, _ = utils.HashPassword(common.AdminPasswd)
		admin.LastLogin = time.UnixMicro(0)
		mysql.DB.Model(User{}).Save(admin)
		l.Log.Warnf("The default administrator account is '%s' and the password is '%s'",
			common.AdminUser, common.AdminPasswd)
	}

	// 默认的config配置
	err = mysql.DB.Model(Config{}).Count(&count).Error
	if err == nil && count == 0 {
		c := new(Config)
		c.ID = 1
		c.ClusterExcludeDatabase = "information_schema,mysql,performance_schema,sys"
		c.TaskMaxParallel = 3
		c.TaskTimeout = 604800 // 7天

		// 每个集群最多只允许同时运行1个任务
		c.TaskConflictLevel = common.TaskConflictLevelCluster
		c.TaskConflictMax = 1
		c.WorkflowRetentionDays = 30 // 工作流记录默认保留30天

		c.SourceStatusDetect = false
		c.SourceStatusDetectDiskUsage = 95 // 磁盘使用率大于95%自动停止工作流

		mysql.DB.Where("id=1").Save(c)
		l.Log.Warnf("use defaule config(%+v)", c)
	}
}

type JSON []byte

func (j *JSON) Scan(value interface{}) error {
	if value == nil {
		*j = nil
		return nil
	}
	s, ok := value.([]byte)
	if !ok {
		return errors.New("invalid scan source")
	}
	*j = append((*j)[0:0], s...)
	return nil
}

func (j JSON) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, nil
	}
	return string(j), nil
}

func (j JSON) MarshalJSON() ([]byte, error) {
	if j == nil {
		return []byte("null"), nil
	}
	return j, nil
}

func (j *JSON) UnmarshalJSON(data []byte) error {
	if j == nil {
		return errors.New("null point exception")
	}
	*j = append((*j)[0:0], data...)
	return nil
}

func (j JSON) String() string {
	if j == nil {
		return ""
	}
	return string(j)
}
