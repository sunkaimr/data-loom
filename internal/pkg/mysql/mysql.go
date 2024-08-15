package mysql

import (
	"fmt"
	"github.com/sunkaimr/data-loom/configs"
	l "github.com/sunkaimr/data-loom/internal/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm/schema"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	glog "gorm.io/gorm/logger"
)

var DB *gorm.DB

var GormLoggerCfg = glog.Config{
	SlowThreshold:             time.Second, // Slow SQL threshold
	LogLevel:                  glog.Silent, // Log level
	IgnoreRecordNotFoundError: true,        // Ignore ErrRecordNotFound error for logger
	Colorful:                  false,       // Disable color
}

func NewMysqlDB(config *configs.Mysql) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		config.User,
		config.Password,
		config.Host,
		config.Port,
		config.DataBase,
	)

	db, err := gorm.Open(
		mysql.New(mysql.Config{
			DSN:                       dsn,
			DefaultStringSize:         256,
			SkipInitializeWithVersion: false,
		}),
		&gorm.Config{
			Logger: glog.New(
				&GormLogger{
					Log: l.Log,
				},
				GormLoggerCfg,
			),
			NamingStrategy: schema.NamingStrategy{
				SingularTable: true,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("connect mysql failed, %s", err)
	}
	DB = db
	return nil
}

type GormLogger struct {
	Log *zap.SugaredLogger
}

func (c *GormLogger) Printf(format string, args ...interface{}) {
	c.Log.Infof(format, args...)
}

func (c *GormLogger) WithLog() *gorm.DB {
	return DB.Session(
		&gorm.Session{
			Logger: glog.New(
				&GormLogger{
					Log: c.Log,
				},
				GormLoggerCfg,
			),
		},
	)
}
