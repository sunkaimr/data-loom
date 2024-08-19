package workflow

import (
	"github.com/sunkaimr/data-loom/configs"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/workflow/types"
	"github.com/sunkaimr/data-loom/pkg/workflow/argo"
	"github.com/sunkaimr/data-loom/pkg/workflow/docker"
	"github.com/sunkaimr/data-loom/pkg/workflow/mock"
)

type Workflow interface {
	// TruncateData 清空数据
	TruncateData(*common.Context, *types.TruncateParaStruct) (string, error)

	// DeleteData 删除数据
	DeleteData(*common.Context, *types.DeleteParaStruct) (string, error)

	// BackupAndDeleteData 备份后删除数据
	//BackupAndDeleteData(*common.Context, any) error

	// ArchiveData 归档数据
	ArchiveData(*common.Context, *types.ArchiveParaStruct) (string, error)

	// RebuildTables 重建表
	RebuildTables(*common.Context, *types.RebuildParaStruct) (string, error)

	// WorkFlowStatus 查看工作流状态
	WorkFlowStatus(*common.Context, string) (types.WorkFlowStatusType, error)

	// WorkFlowAddr 查看工作流地址
	WorkFlowAddr(*common.Context, string) (string, error)

	// DeleteWorkFlow 删除工作流
	DeleteWorkFlow(*common.Context, string) error

	// StopWorkFlow 停止工作流
	StopWorkFlow(ctx *common.Context, workflow string) error
}

func NewDriver(driver string) Workflow {
	switch driver {
	case "argo":
		return &argo.Argo{
			URL:  configs.C.WorkFlow.Argo.URL,
			Auth: configs.C.WorkFlow.Argo.Token,
		}
	case "docker":
		return &docker.Docker{}
	case "mock":
		return &mock.Mock{}
	default:
		return &mock.Mock{}
	}
}
