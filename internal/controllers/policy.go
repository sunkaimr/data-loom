package controllers

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/services"
	"net/http"
)

type PolicyController struct{}

// CreatePolicy			创建策略
// @Router				/policy [post]
// @Description			创建策略
// @Tags				策略
// @Param				Policy		body		services.PolicyService	true	"Policy"
// @Success				200			{object}	common.Response{data=services.PolicyService}
// @Failure				500			{object}	common.Response
func (c *PolicyController) CreatePolicy(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)

	req := &services.PolicyService{}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr, Error: err.Error()})
		return
	}

	// 参数校验
	if ok, res, err := req.CheckParameters(ctx); !ok {
		log.Errorf("check parameters(%+v) not pass, %s", req, err)
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}

	res, err := req.CreatePolicy(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: req})
	return
}

// UpdatePolicy			更新策略
// @Router				/policy [put]
// @Description			更新策略
// @Tags				策略
// @Param				Policy		body		services.PolicyService	true	"Policy"
// @Success				200			{object}	common.Response{data=services.PolicyService}
// @Failure				500			{object}	common.Response
func (c *PolicyController) UpdatePolicy(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)

	req := &services.PolicyService{
		Model: services.Model{
			ID: common.InvalidUint,
		},
		SrcID:  common.InvalidUint,
		DestID: common.InvalidUint,
		Day:    common.InvalidInt,
	}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr})
		return
	}

	ok, res, err := req.CheckUpdateParameters(ctx)
	if !ok {
		log.Errorf("check update parameters no paas, %s", err)
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}

	res, err = req.UpdatePolicy(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}

	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: req})
	return
}

// QueryPolicy			查询策略
// @Router				/policy [get]
// @Description			查询策略
// @Tags				策略
// @Param   			page			query		int			false  	"page"
// @Param   			pageSize		query		int     	false  	"pageSize"
// @Param   			id				query		uint     	false  	"策略ID"
// @Param   			creator			query		string     	false  	"创建人"
// @Param   			editor			query		string     	false  	"修改人"
// @Param   			name			query		string     	false  	"策略名称"
// @Param   			description		query		string     	false  	"描述"
// @Param   			enable			query		bool     	false  	"是否生效"
// @Param   			period			query		string     	false  	"执行周期"		Enums(once,monthly,quarterly,six-months,yearly)
// @Param   			pause			query		bool     	false  	"执行窗口外是否需要暂停执行"
// @Param   			rebuild_flag	query		bool     	false  	"执行窗口外是否重建表(仅在治理方式是删除时有效)。true:在执行窗口外仍然走重建流程; false:执行窗口外跳过重建流程"
// @Param   			cleaning_speed	query		string     	false  	"清理速度"		Enums(steady,balanced,swift)
// @Param   			src_id			query		uint     	false  	"源端ID"
// @Param   			govern			query		string     	false  	"数据治理方式"	Enums(truncate,delete,backup-delete,archive)
// @Param   			condition		query		string     	false  	"数据治理条件"
// @Param   			retain_src_data	query		bool     	false  	"归档时否保留源表数据"
// @Param   			dest_id			query		uint     	false  	"目标端ID"
// @Param   			relevant		query		string     	false  	"关注人"
// @Param   			notify_policy	query		string     	false  	"通知策略"		Enums(silence,success,failed,always)
// @Success				200		{object}	common.Response{data=services.PolicyService}
// @Failure				500		{object}	common.Response
func (c *PolicyController) QueryPolicy(ctx *gin.Context) {
	id := common.ParsingQueryUintID(ctx.Query("id"))
	srcID := common.ParsingQueryUintID(ctx.Query("src_id"))
	destID := common.ParsingQueryUintID(ctx.Query("dest_id"))

	queryMap := make(map[string]string, 10)
	queryMap["creator"] = ctx.Query("creator")
	queryMap["editor"] = ctx.Query("editor")
	queryMap["name"] = ctx.Query("name")
	queryMap["description"] = ctx.Query("description")
	queryMap["bu"] = ctx.Query("bu")
	queryMap["editor"] = ctx.Query("editor")
	queryMap["period"] = ctx.Query("period")
	queryMap["cleaning_speed"] = ctx.Query("cleaning_speed")
	queryMap["govern"] = ctx.Query("govern")
	queryMap["condition"] = ctx.Query("condition")
	queryMap["relevant"] = ctx.Query("relevant")
	queryMap["notify_policy"] = ctx.Query("notify_policy")

	policy := services.PolicyService{
		Model: services.Model{
			ID: id,
		},
		SrcID:  srcID,
		DestID: destID,
	}
	data, res, err := policy.QueryPolicy(ctx, queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: data})
	return
}

// DeletePolicy			删除策略
// @Router				/policy [delete]
// @Description			删除策略
// @Tags				策略
// @Param				Policy		body		services.PolicyService	true	"Policy"
// @Success				200			{object}	common.Response{data=services.PolicyService}
// @Failure				500			{object}	common.Response
func (c *PolicyController) DeletePolicy(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)
	req := &services.PolicyService{
		Model: services.Model{
			ID: common.InvalidUint,
		},
	}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr})
		return
	}
	// 参数校验
	if common.InvalidUintID(req.ID) && len(req.Name) == 0 {
		err := fmt.Errorf("validate Policy id(%v) or name(%v) not pass", req.ID, req.Name)
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeDestNameLenErr, Error: err.Error()})
		return
	}

	res, err := req.DeletePolicy(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res})
	return
}
