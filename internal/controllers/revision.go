package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/services"
)

type PolicyRevisionController struct{}
type TaskRevisionController struct{}

// QueryPolicyRevision	查询策略修订记录
// @Router				/policy/revision [get]
// @Description			查询策略修订记录
// @Tags				策略
// @Param   			page			query		int			false  	"page"
// @Param   			pageSize		query		int     	false  	"pageSize"
// @Param   			id				query		uint     	false  	"ID"
// @Param   			editor			query		string     	false  	"修改人"
// @Param   			policy_id		query		uint     	false  	"策略ID"
// @Param   			modify_field	query		string     	false  	"修改字段"
// @Param   			old_value		query		string     	false  	"原始值"
// @Param   			new_value		query		string     	false  	"修改值"
// @Success				200		{object}	common.Response{data=services.PolicyRevisionService}
// @Failure				500		{object}	common.Response
func (c *PolicyRevisionController) QueryPolicyRevision(ctx *gin.Context) {
	id := common.ParsingQueryUintID(ctx.Query("id"))
	policyID := common.ParsingQueryUintID(ctx.Query("policy_id"))

	queryMap := make(map[string]string, 10)
	queryMap["editor"] = ctx.Query("editor")
	queryMap["modify_field"] = ctx.Query("modify_field")
	queryMap["old_value"] = ctx.Query("old_value")
	queryMap["new_value"] = ctx.Query("new_value")

	policy := services.PolicyRevisionService{
		Model: services.Model{
			ID: id,
		},
		PolicyID: policyID,
	}
	data, code, err := policy.QueryPolicyRevision(ctx, queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// QueryPolicyRevision	查询任务修订记录
// @Router				/task/revision [get]
// @Description			查询任务修订记录
// @Tags				任务
// @Param   			page			query		int			false  	"page"
// @Param   			pageSize		query		int     	false  	"pageSize"
// @Param   			id				query		uint     	false  	"ID"
// @Param   			editor			query		string     	false  	"修改人"
// @Param   			task_id			query		uint     	false  	"任务ID"
// @Param   			modify_field	query		string     	false  	"修改字段"
// @Param   			old_value		query		string     	false  	"原始值"
// @Param   			new_value		query		string     	false  	"修改值"
// @Success				200		{object}	common.Response{data=services.PolicyRevisionService}
// @Failure				500		{object}	common.Response
func (c *TaskRevisionController) QueryPolicyRevision(ctx *gin.Context) {
	id := common.ParsingQueryUintID(ctx.Query("id"))
	taskID := common.ParsingQueryUintID(ctx.Query("task_id"))

	queryMap := make(map[string]string, 10)
	queryMap["editor"] = ctx.Query("editor")
	queryMap["modify_field"] = ctx.Query("modify_field")
	queryMap["old_value"] = ctx.Query("old_value")
	queryMap["new_value"] = ctx.Query("new_value")

	task := services.TaskRevisionService{
		Model: services.Model{
			ID: id,
		},
		TaskID: taskID,
	}
	data, code, err := task.QueryTaskRevision(ctx, queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}
