package controllers

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/services"
	"github.com/sunkaimr/data-loom/internal/workflow/types"
	"net/http"
)

type TaskController struct{}

// UpdateTask			更新任务
// @Router				/task [put]
// @Description			更新任务
// @Tags				任务
// @Param				Task		body		services.TaskService	true	"Task"
// @Success				200			{object}	common.Response{data=services.TaskService}
// @Failure				500			{object}	common.Response
func (c *TaskController) UpdateTask(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)

	req := &services.TaskService{Model: services.Model{ID: common.InvalidUint}}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr})
		return
	}

	ok, code, err := req.CheckUpdateParameters(ctx)
	if !ok {
		log.Errorf("check update parameters no paas, %s", err)
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}

	code, err = req.UpdateTask(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: req})
	return
}

// QueryTask			查询任务
// @Router				/task [get]
// @Description			查询任务
// @Tags				任务
// @Param   			page				query		int			false  	"page"
// @Param   			pageSize			query		int     	false  	"pageSize"
// @Param   			id					query		uint     	false  	"任务ID"
// @Param   			creator				query		string     	false  	"创建人"
// @Param   			editor				query		string     	false  	"修改人"
// @Param   			name				query		string     	false  	"任务名称"
// @Param   			description			query		string     	false  	"描述"
// @Param   			enable				query		bool     	false  	"是否生效"
// @Param   			policy_id			query		int     	false  	"策略ID"
// @Param   			execute_date		query		string     	false  	"计划执行日期: 2024-03-01"
// @Param   			pause				query		bool     	false  	"执行窗口外是否需要暂停执行"
// @Param   			rebuild_flag		query		bool     	false  	"执行窗口外是否重建表(仅在治理方式是删除时有效)。true:在执行窗口外仍然走重建流程; false:执行窗口外跳过重建流程"
// @Param   			task_status			query		int     	false  	"任务状态"
// @Param   			task_reason			query		string     	false  	"任务失败原因"
// @Param   			task_detail			query		string     	false  	"任务失败详情"
// @Param   			workflow			query		string     	false  	"工作流"
// @Param   			src_id				query		uint     	false  	"源端ID"
// @Param   			src_name			query		string     	false  	"源端名称"
// @Param   			src_bu				query		string     	false  	"资产BU"
// @Param   			src_cluster_name	query		string     	false  	"源端集群名称"
// @Param   			src_cluster_id		query		string     	false  	"源端集群ID"
// @Param   			src_database_name	query		string     	false  	"源库名"
// @Param   			src_tables_name		query		string     	false  	"源表名"
// @Param   			src_columns			query		string     	false  	"源端归档列名"
// @Param   			govern				query		string     	false  	"数据治理方式"		Enums(truncate,delete,backup-delete,archive)
// @Param   			condition			query		string     	false  	"数据治理条件"
// @Param   			clean_src			query		bool     	false  	"是否清理源表"
// @Param   			cleaning_speed		query		string     	false  	"清理速度"			Enums(steady,balanced,swift)
// @Param   			dest_id				query		uint     	false  	"目标端ID"
// @Param   			dest_name			query		string     	false  	"目标端名称"
// @Param   			dest_storage		query		string     	false  	"存储介质"			Enums(mysql, databend)
// @Param   			dest_connection_id	query		uint     	false  	"目标端连接ID"
// @Param   			dest_database_name	query		string     	false  	"目标端数据库"
// @Param   			dest_table_name		query		string     	false  	"目标端表名字"
// @Param   			dest_compress		query		bool     	false  	"目标端是否压缩存储存储"
// @Param   			relevant			query		string     	false  	"关注人"
// @Param   			notify_policy		query		string     	false  	"通知策略"			Enums(silence,success,failed,always)
// @Success				200		{object}	common.Response{data=services.TaskService}
// @Failure				500		{object}	common.Response
func (c *TaskController) QueryTask(ctx *gin.Context) {
	id := common.ParsingQueryUintID(ctx.Query("id"))
	policyID := common.ParsingQueryUintID(ctx.Query("policy_id"))
	srcID := common.ParsingQueryUintID(ctx.Query("src_id"))
	destID := common.ParsingQueryUintID(ctx.Query("dest_id"))
	destConnectionID := common.ParsingQueryUintID(ctx.Query("dest_connection_id"))

	queryMap := make(map[string]string, 10)
	queryMap["creator"] = ctx.Query("creator")
	queryMap["editor"] = ctx.Query("editor")
	queryMap["name"] = ctx.Query("name")
	queryMap["description"] = ctx.Query("description")
	queryMap["execute_date"] = ctx.Query("execute_date")
	queryMap["task_reason"] = ctx.Query("task_reason")
	queryMap["task_detail"] = ctx.Query("task_detail")
	queryMap["workflow"] = ctx.Query("workflow")
	queryMap["src_name"] = ctx.Query("src_name")
	queryMap["src_bu"] = ctx.Query("src_bu")
	queryMap["src_cluster_name"] = ctx.Query("src_cluster_name")
	queryMap["src_cluster_id"] = ctx.Query("src_cluster_id")
	queryMap["src_database_name"] = ctx.Query("src_database_name")
	queryMap["src_tables_name"] = ctx.Query("src_tables_name")
	queryMap["govern"] = ctx.Query("govern")
	queryMap["condition"] = ctx.Query("condition")
	queryMap["cleaning_speed"] = ctx.Query("cleaning_speed")
	queryMap["dest_name"] = ctx.Query("dest_name")
	queryMap["dest_storage"] = ctx.Query("dest_storage")
	queryMap["dest_database_name"] = ctx.Query("dest_database_name")
	queryMap["dest_table_name"] = ctx.Query("dest_table_name")
	queryMap["relevant"] = ctx.Query("relevant")
	queryMap["notify_policy"] = ctx.Query("notify_policy")

	task := services.TaskService{
		Model:            services.Model{ID: id},
		PolicyID:         policyID,
		SrcID:            srcID,
		DestID:           destID,
		DestConnectionID: destConnectionID,
	}
	data, code, err := task.QueryTask(ctx, queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// DeleteTask			删除任务
// @Router				/task [delete]
// @Description			删除任务
// @Tags				任务
// @Param				Task		body		services.TaskService	true	"Task"
// @Success				200			{object}	common.Response{data=services.TaskService}
// @Failure				500			{object}	common.Response
func (c *TaskController) DeleteTask(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)
	req := &services.TaskService{Model: services.Model{ID: common.InvalidUint}}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr})
		return
	}
	// 参数校验
	if common.InvalidUintID(req.ID) {
		err := fmt.Errorf("validate ask id(%v) not pass", req.ID)
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeDestNameLenErr, Error: err.Error()})
		return
	}

	code, err := req.DeleteTask(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code})
	return
}

// UpdateTaskResult		上报任务执行结果
// @Router				/task/result [put]
// @Description			上报任务执行结果
// @Tags				任务
// @Param				TaskResult	body		types.TaskResultService	true	"TaskResult"
// @Success				200			{object}	common.Response{data=services.TaskService}
// @Failure				500			{object}	common.Response
func (c *TaskController) UpdateTaskResult(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)
	user := common.ExtractUserInfo(ctx)

	req := &types.TaskResultService{
		ID:                 common.InvalidUint,
		TaskResultQuantity: common.InvalidInt,
		TaskResultSize:     common.InvalidInt,
	}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr})
		return
	}

	// 校验权限（只有admin或专用token才能更新）
	if !(user.UserName == common.AdminUser || (user.UserName == common.UpdateTaskResultUser && user.UserID == req.ID)) {
		err := fmt.Errorf("user(%s) does not have permission update task(%v) result, only '%s' or '%s' for task(%v) can operate",
			user.UserName, req.ID, common.SystemUser, common.UpdateTaskResultUser, user.UserID)
		log.Error(err)
		ctx.JSON(http.StatusForbidden, common.Response{ServiceCode: common.CodeTaskStatusUpdateDenied, Error: err.Error()})
		return
	}

	res, code, err := services.UpdateTaskResult(ctx, req)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: res})
	return
}
