package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/services"
	"net/http"
)

type SourceController struct{}

// CreateSource     创建源端信息
// @Router			/source [post]
// @Description		创建源端信息
// @Tags			源端信息
// @Param			Source	body		services.SourceService	true	"Source"
// @Success			200		{object}	common.Response{data=services.SourceService}
// @Failure			500		{object}	common.Response
func (c *SourceController) CreateSource(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)

	req := &services.SourceService{}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr, Error: err.Error()})
		return
	}

	// 参数校验
	if ok, code, err := req.CheckParameters(ctx); !ok {
		log.Errorf("check parameters(%+v) not pass, %s", req, err)
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}

	code, err := req.CreateSource(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: req})
	return
}

// UpdateSource     更新源端信息
// @Router			/source [put]
// @Description		创建源端信息
// @Tags			源端信息
// @Param			Source	body		services.SourceService	true	"Source"
// @Success			200		{object}	common.Response{data=services.SourceService}
// @Failure			500		{object}	common.Response
func (c *SourceController) UpdateSource(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)

	req := &services.SourceService{Model: services.Model{ID: common.InvalidUint}}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr})
		return
	}

	if common.InvalidUintID(req.ID) {
		log.Errorf("invalid Source.id(%d)", req.ID)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeInvalidID})
		return
	}

	// 参数校验
	if len(req.Name) == 0 || len(req.Name) == 1024 {
		log.Errorf("validate source name(%s) not pass", req.Name)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeSourceNameErr})
		return
	}

	code, err := req.UpdateSource(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: req})
	return
}

// DeleteSource     删除源端信息
// @Router			/source [delete]
// @Description		删除源端信息
// @Tags			源端信息
// @Param			Source	body		services.SourceService	true	"Source"
// @Success			200		{object}	common.Response
// @Failure			500		{object}	common.Response
func (c *SourceController) DeleteSource(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)
	req := &services.SourceService{Model: services.Model{ID: common.InvalidUint}}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr})
		return
	}
	// 参数校验
	if common.InvalidUintID(req.ID) && len(req.Name) == 0 {
		log.Errorf("validate source id(%d) or name(%s) not pass", req.ID, req.Name)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeSourceParamErr})
		return
	}

	code, err := req.DeleteSource(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code})
	return
}

// QuerySource      查询源端信息
// @Router			/source [get]
// @Description		查询源端信息
// @Tags			源端信息
// @Param   		page			query		int			false  	"page"
// @Param   		pageSize		query		int     	false  	"pageSize"
// @Param   		id				query		uint     	false  	"源端ID"
// @Param   		creator			query		string     	false  	"创建人"
// @Param   		editor			query		string     	false  	"修改人"
// @Param   		name			query		string     	false  	"源端名字"
// @Param   		description		query		string     	false  	"说明"
// @Param   		bu				query		string     	false  	"bu"
// @Param   		cluster_name	query		string     	false  	"service名称"
// @Param   		cluster_id		query		string     	false  	"cluster_id"
// @Param   		database_name	query		string     	false  	"数据库"
// @Param   		tables_name		query		string     	false  	"表名字"
// @Success			200		{object}	common.Response{data=services.SourceService}
// @Failure			500		{object}	common.Response
func (c *SourceController) QuerySource(ctx *gin.Context) {
	id := common.ParsingQueryUintID(ctx.Query("id"))
	queryMap := make(map[string]string, 10)
	queryMap["creator"] = ctx.Query("creator")
	queryMap["editor"] = ctx.Query("editor")
	queryMap["name"] = ctx.Query("name")
	queryMap["description"] = ctx.Query("description")
	queryMap["bu"] = ctx.Query("bu")
	queryMap["cluster_name"] = ctx.Query("cluster_name")
	queryMap["cluster_id"] = ctx.Query("cluster_id")
	queryMap["database_name"] = ctx.Query("database_name")
	queryMap["tables_name"] = ctx.Query("tables_name")
	queryMap["columns"] = ctx.Query("columns")

	source := services.SourceService{Model: services.Model{ID: id}}
	data, code, err := source.QuerySource(ctx, queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}
