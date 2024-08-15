package unsupported

import (
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
)

var UnSupportClusterType = errors.New("unsupported cluster type")

type ClusterUnknown struct {
}

func (c *ClusterUnknown) ClusterInfo(_ *gin.Context) (string, string, string, string, common.ServiceCode, error) {
	return "", "", "", "", common.CodeServerErr, UnSupportClusterType
}

// TestConnect 测试集群是否可达
func (c *ClusterUnknown) TestConnect(_ *gin.Context) (common.ServiceCode, error) {
	return common.CodeServerErr, UnSupportClusterType
}

// GetDatabases 查询库列表
func (c *ClusterUnknown) GetDatabases(_ *gin.Context) ([]string, common.ServiceCode, error) {
	return nil, common.CodeServerErr, UnSupportClusterType
}

// GetTables 查询表列表
func (c *ClusterUnknown) GetTables(_ *gin.Context, _ string) ([]string, common.ServiceCode, error) {
	return nil, common.CodeServerErr, UnSupportClusterType
}

func (c *ClusterUnknown) TablesHasPrimaryKey(_ *gin.Context, _ string, _ []string) (bool, common.ServiceCode, error) {
	return false, common.CodeServerErr, UnSupportClusterType
}

func (c *ClusterUnknown) SQLExplain(_ *gin.Context, _ string, _ string, _ string, _ string /*database table columns conditions*/) (int64, common.ServiceCode, error) {
	return 0, common.CodeServerErr, UnSupportClusterType
}

func (c *ClusterUnknown) GetFreeDisk(_ *gin.Context) (int, error) {
	return 0, UnSupportClusterType
}

func (c *ClusterUnknown) GetTablesSize(_ *gin.Context, _ string, _ []string /*database []tables*/) (int, common.ServiceCode, error) {
	return 0, common.CodeServerErr, UnSupportClusterType
}

func (c *ClusterUnknown) GetDiskUsage(_ *gin.Context) (int, error) {
	return 0, UnSupportClusterType
}

func (c *ClusterUnknown) GetDiskUsed(_ *gin.Context) (int, error) {
	return 0, UnSupportClusterType
}

func (c *ClusterUnknown) GetClusterBigTables(*gin.Context, int) (any, common.ServiceCode, error) {
	return nil, common.CodeServerErr, UnSupportClusterType
}
