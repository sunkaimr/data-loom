package cluster

import (
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
)

type ClusterDriver interface {
	// ClusterInfo 获取集群的连接信息
	ClusterInfo(*gin.Context) (string, string, string, string, common.ServiceCode, error)

	// TestConnect 测试集群是否可达
	TestConnect(*gin.Context) (common.ServiceCode, error)

	// GetDatabases 查询库列表
	GetDatabases(*gin.Context) ([]string, common.ServiceCode, error)

	// GetTables 查询表列表
	GetTables(*gin.Context, string /*database*/) ([]string, common.ServiceCode, error)

	// TablesHasPrimaryKey 检查表是否都有主键
	TablesHasPrimaryKey(*gin.Context, string, []string /*database []tables*/) (bool, common.ServiceCode, error)

	// SQLExplain 检查归档条件合法性
	SQLExplain(*gin.Context, string, string, string, string /*database table columns conditions*/) (int64, common.ServiceCode, error)

	// GetClusterBigTables 统计集群的大表
	GetClusterBigTables(*gin.Context, int /* 只抓取表大于10G以上的 */) (any, common.ServiceCode, error)

	// GetFreeDisk 检查集群的剩余磁盘空间
	GetFreeDisk(*gin.Context) (int, error)

	// GetTablesSize 获取表大小
	GetTablesSize(*gin.Context, string, []string /*database []tables*/) (int, common.ServiceCode, error)

	// GetDiskUsage 获取磁盘使用率
	GetDiskUsage(*gin.Context) (int, error)

	// GetDiskUsed 获取磁盘使用空间
	GetDiskUsed(ctx *gin.Context) (int, error)
}

type ClusterSynchronizer interface {
	// Sync 同步集群信息
	Sync(*gin.Context) ([]*models.Cluster, common.ServiceCode, error)
}
