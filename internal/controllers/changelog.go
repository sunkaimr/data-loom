package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/services"
)

type TaskChangeLogController struct{}

// QueryTaskChangeLog	查询任务状态变更历史极力
// @Router				/task/changelog [get]
// @Description			查询任务状态变更历史极力
// @Tags				策略
// @Param   			page			query		int			false  	"page"
// @Param   			pageSize		query		int     	false  	"pageSize"
// @Param   			task_id			query		uint     	false  	"任务ID"
// @Success				200		{object}	common.Response{data=services.TaskChangeLogService}
// @Failure				500		{object}	common.Response
func (c *TaskChangeLogController) QueryTaskChangeLog(ctx *gin.Context) {
	changelog := services.TaskChangeLogService{
		TaskID: common.ParsingQueryUintID(ctx.Query("task_id")),
	}
	data, code, err := changelog.QueryTaskChangeLog(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}
