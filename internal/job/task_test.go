package job

import (
	"context"
	"fmt"
	"github.com/sunkaimr/data-loom/configs"
	"github.com/sunkaimr/data-loom/internal/middlewares"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/pkg/logger"
	"github.com/sunkaimr/data-loom/internal/pkg/mysql"
	"github.com/sunkaimr/data-loom/internal/services"
	"go.uber.org/zap"
	"strings"
	"testing"
	"time"
)

func init() {
	// 读取配置
	configs.Init("../../config.yaml")
	logger.Init()

	// 初始化数据库
	if err := mysql.NewMysqlDB(&configs.C.Mysql); err != nil {
		panic(err)
	} else {
		models.UpdateModels()
	}
}

func Test(t *testing.T) {
	log := logger.AddFields(logger.Log, zap.String(common.RequestID, time.Now().Format("20060102150405")+strings.Repeat("0", 6)))
	ctxCancel, cancel := context.WithCancel(context.TODO())
	db := (&mysql.GormLogger{Log: log}).WithLog()

	ctx := common.NewContext().WithContext(ctxCancel).WithCancel(cancel).WithLog(log).WithDB(db)
	ginCtx := middlewares.NewGinContext(ctx.Log, ctx.DB)

	var cluster []models.Cluster
	if err := db.Model(models.Cluster{}).Find(&cluster).Error; err != nil {
		log.Error(err)
		return
	}

	for _, c := range cluster {
		//if c.ClusterID != "mysql_40c3124219" {
		//	continue
		//}

		clusterSvc := &services.ClusterService{}
		clusterSvc.ModelToService(&c)

		//diskUsage, err := services.NewClusterDriver(clusterSvc).GetDiskUsed(ginCtx)
		diskUsage, err := services.NewClusterDriver(clusterSvc).GetFreeDisk(ginCtx)
		if err != nil {
			//log.Errorf("get cluster(%s) disk usage failed, %s", clusterSvc.ClusterID, err)
			fmt.Printf("%s,%s,%d\n", clusterSvc.ClusterID, clusterSvc.ClusterName, -1)
			continue
		}
		fmt.Printf("%s,%s,%d\n", clusterSvc.ClusterID, clusterSvc.ClusterName, diskUsage)
	}
}

func Test1(t *testing.T) {
	log := logger.AddFields(logger.Log, zap.String(common.RequestID, time.Now().Format("20060102150405")+strings.Repeat("0", 6)))
	ctxCancel, cancel := context.WithCancel(context.TODO())
	db := (&mysql.GormLogger{Log: log}).WithLog()

	ctx := common.NewContext().WithContext(ctxCancel).WithCancel(cancel).WithLog(log).WithDB(db)

	GrabClusterTableSize(ctx)
}
