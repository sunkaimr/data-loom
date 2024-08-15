package unsupported

import (
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
)

var NotSupportSync = errors.New("not support synchronizer")

type Sync struct {
}

func (c *Sync) Sync(_ *gin.Context) ([]*models.Cluster, common.ServiceCode, error) {
	// 按此格式返回即可
	//clusters := make([]*models.Cluster, 0, len(services))
	//clusters = append(clusters, &models.Cluster{
	//	Bu:          "Bu",
	//	Env:         "test",
	//	Description: "Description",
	//	ImportFrom:  "xxxx",
	//	ClusterType: common.ClusterTypeMysql,
	//	ClusterName: "ClusterName",
	//	ClusterID:   "ClusterID",
	//	ServiceAddr: "1.2.3.4:3306",
	//	WriteAddr:   "1.2.3.5:3306",
	//	ReadAddr:    "1.2.3.6:3306",
	//})
	//return clusters, common.CodeOK, nil

	return nil, common.CodeServerErr, NotSupportSync
}
