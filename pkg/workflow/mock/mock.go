package mock

import (
	"encoding/json"
	"fmt"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/workflow/types"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"net/http"
	"time"
)

type Mock struct{}

func (c *Mock) TruncateData(ctx *common.Context, para *types.TruncateParaStruct) (string, error) {
	log := ctx.Log
	log.Debugf("sending TruncateData request to mock, %+v", para)

	go func() {
		// 模拟工作流执行
		time.Sleep(80 * time.Second)

		url := para.Callback.URL
		headers := make(map[string]string, 1)
		headers["Authorization"] = "Bearer " + para.Callback.Token

		para.CallbackResult.ID = para.TaskID
		para.CallbackResult.TaskStatus = common.TaskStatusSuccess
		para.CallbackResult.TaskReason = "模拟调用清空数据工作流"
		para.CallbackResult.TaskResultQuantity = 111111
		b, _ := json.Marshal(para.CallbackResult)

		_, _, err := utils.HttpDo(http.MethodPut, url, headers, nil, string(b))
		if err != nil {
			log.Error("report TruncateData result failed, %+v", err)
			return
		}

		log.Infof("report TruncateData result success")
	}()

	return "mock_truncate", nil
}

func (c *Mock) DeleteData(ctx *common.Context, para *types.DeleteParaStruct) (string, error) {
	log := ctx.Log
	log.Debugf("sending DeleteData request to mock, %+v", para)

	go func() {
		// 模拟工作流执行
		time.Sleep(80 * time.Second)

		url := para.Callback.URL
		headers := make(map[string]string, 1)
		headers["Authorization"] = "Bearer " + para.Callback.Token

		para.CallbackResult.ID = para.TaskID
		para.CallbackResult.TaskStatus = common.TaskStatusSuccess
		para.CallbackResult.TaskReason = "模拟调用删除数据工作流"
		para.CallbackResult.TaskResultQuantity = 222222
		b, _ := json.Marshal(para.CallbackResult)

		_, _, err := utils.HttpDo(http.MethodPut, url, headers, nil, string(b))
		if err != nil {
			log.Error("report DeleteData result failed, %+v", err)
			return
		}

		log.Infof("report DeleteData result success")
	}()

	return "mock_delete", nil
}

func (c *Mock) ArchiveData(ctx *common.Context, para *types.ArchiveParaStruct) (string, error) {
	log := ctx.Log
	log.Debugf("sending ArchiveData request to mock, %+v", para)

	go func() {
		// 模拟工作流执行
		time.Sleep(80 * time.Second)

		url := para.Callback.URL
		headers := make(map[string]string, 1)
		headers["Authorization"] = "Bearer " + para.Callback.Token

		para.CallbackResult.ID = para.TaskID
		para.CallbackResult.TaskStatus = common.TaskStatusSuccess
		para.CallbackResult.TaskReason = "模拟调用归档工作流"
		para.CallbackResult.TaskResultQuantity = 222222
		b, _ := json.Marshal(para.CallbackResult)

		_, _, err := utils.HttpDo(http.MethodPut, url, headers, nil, string(b))
		if err != nil {
			log.Error("report ArchiveData result failed, %+v", err)
			return
		}

		log.Infof("report ArchiveData result success")
	}()

	return "mock_archive", nil
}

func (c *Mock) RebuildTables(ctx *common.Context, para *types.RebuildParaStruct) (string, error) {
	log := ctx.Log
	log.Debugf("sending RebuildTables request to mock, %+v", para)

	go func() {
		// 模拟工作流执行
		time.Sleep(80 * time.Second)

		url := para.Callback.URL
		headers := make(map[string]string, 1)
		headers["Authorization"] = "Bearer " + para.Callback.Token

		para.CallbackResult.ID = para.TaskID
		para.CallbackResult.TaskStatus = common.TaskStatusSuccess
		para.CallbackResult.TaskReason = "模拟调用重建表工作流"
		para.CallbackResult.TaskResultQuantity = 222222
		b, _ := json.Marshal(para.CallbackResult)

		_, _, err := utils.HttpDo(http.MethodPut, url, headers, nil, string(b))
		if err != nil {
			log.Error("report RebuildTables result failed, %+v", err)
			return
		}

		log.Infof("report RebuildTables result success")
	}()

	return "mock_rebuild", nil
}

func (c *Mock) WorkFlowStatus(_ *common.Context, id string) (types.WorkFlowStatusType, error) {
	fmt.Printf("mock recv WorkFlowStatus request, %+v", id)
	return types.WorkFlowStatusRunning, nil
}

func (c *Mock) WorkFlowAddr(_ *common.Context, _ string) (string, error) {
	return "", nil
}

func (c *Mock) DeleteWorkFlow(ctx *common.Context, workflow string) error {
	ctx.Log.Infof("mock recv DeleteWorkFlow request, %+v", workflow)
	return nil
}

func (c *Mock) StopWorkFlow(ctx *common.Context, workflow string) error {
	ctx.Log.Infof("mock recv StopWorkFlow request, %+v", workflow)
	return nil
}
