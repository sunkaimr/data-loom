package docker

import (
	"fmt"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/workflow/types"
)

type Docker struct{}

func (c *Docker) TruncateData(ctx *common.Context, para *types.TruncateParaStruct) (string, error) {
	fmt.Printf("docker recv TruncateData request, %+v", para)
	return "", nil
}

func (c *Docker) DeleteData(ctx *common.Context, para *types.DeleteParaStruct) (string, error) {
	fmt.Printf("docker recv DeleteData request, %+v", para)
	return "", nil
}

func (c *Docker) ArchiveData(ctx *common.Context, para *types.ArchiveParaStruct) (string, error) {
	fmt.Printf("docker recv ArchiveData request, %+v", para)
	return "", nil
}

func (c *Docker) RebuildTables(ctx *common.Context, para *types.RebuildParaStruct) (string, error) {
	fmt.Printf("docker recv RebuildTables request, %+v", para)
	return "", nil
}

func (c *Docker) WorkFlowStatus(ctx *common.Context, id string) (types.WorkFlowStatusType, error) {
	fmt.Printf("docker recv WorkFlowStatus request, %+v", id)
	return types.WorkFlowStatusRunning, nil
}

func (c *Docker) WorkFlowAddr(_ *common.Context, workflow string) (string, error) {
	return "", nil
}

func (c *Docker) DeleteWorkFlow(ctx *common.Context, workflow string) error {
	return nil
}

func (c *Docker) StopWorkFlow(ctx *common.Context, workflow string) error {
	return nil
}
