package controllers

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/services"
	"net/http"
)

type DestController struct{}

// CreateDestination	创建目标端信息
// @Router				/dest [post]
// @Description			创建目标端信息
// @Tags				目标端信息
// @Param				Destination	body		services.DestService	true	"Destination"
// @Success				200			{object}	common.Response{data=services.DestService}
// @Failure				500			{object}	common.Response
func (c *DestController) CreateDestination(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)

	req := &services.DestService{}
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

	res, err := req.CreateDest(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: req})
}

// UpdateDestination	更新目标端信息
// @Router				/dest [put]
// @Description			更新目标端信息
// @Tags				目标端信息
// @Param				Destination	body		services.DestService	true	"Destination"
// @Success				200			{object}	common.Response{data=services.DestService}
// @Failure				500			{object}	common.Response
func (c *DestController) UpdateDestination(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)

	req := &services.DestService{
		Model: services.Model{
			ID: common.InvalidUint,
		},
		ConnectionID: common.InvalidUint,
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

	res, err = req.UpdateDest(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: req})
}

// DeleteDestination	删除目标端信息
// @Router				/dest [delete]
// @Description			删除目标端信息
// @Tags				目标端信息
// @Param				Destination	body		services.DestService	true	"Destination"
// @Success				200			{object}	common.Response{data=services.DestService}
// @Failure				500			{object}	common.Response
func (c *DestController) DeleteDestination(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)
	req := &services.DestService{
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
		err := fmt.Errorf("validate Destination id(%v) or name(%v) not pass", req.ID, req.Name)
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeDestNameLenErr, Error: err.Error()})
		return
	}

	res, err := req.DeleteDest(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res})
	return
}

// QueryDestination		查询目标端信息
// @Router				/dest [get]
// @Description			查询目标端信息
// @Tags				目标端信息
// @Param   			page			query		int			false  	"page"
// @Param   			pageSize		query		int     	false  	"pageSize"
// @Param   			id				query		uint     	false  	"目标端ID"
// @Param   			creator			query		string     	false  	"创建人"
// @Param   			editor			query		string     	false  	"修改人"
// @Param   			name			query		string     	false  	"目标端名字"
// @Param   			description		query		string     	false  	"描述"
// @Param   			storage			query		string     	false  	"存储介质"			Enums(mysql, databend)
// @Param   			connection_id	query		uint     	false  	"连接ID"
// @Param   			database_name	query		string     	false  	"数据库"
// @Param   			tables_name		query		string     	false  	"表名字"
// @Param   			compress		query		bool     	false  	"是否压缩存储存储"
// @Success				200		{object}	common.Response{data=services.DestService}
// @Failure				500		{object}	common.Response
func (c *DestController) QueryDestination(ctx *gin.Context) {
	id := common.ParsingQueryUintID(ctx.Query("id"))
	connID := common.ParsingQueryUintID(ctx.Query("connection_id"))

	queryMap := make(map[string]string, 10)
	queryMap["creator"] = ctx.Query("creator")
	queryMap["editor"] = ctx.Query("editor")
	queryMap["name"] = ctx.Query("name")
	queryMap["description"] = ctx.Query("description")
	queryMap["storage"] = ctx.Query("storage")
	queryMap["database_name"] = ctx.Query("database_name")
	queryMap["tables_name"] = ctx.Query("tables_name")

	dest := services.DestService{
		Model: services.Model{
			ID: id,
		},
		ConnectionID: connID,
	}
	data, res, err := dest.QueryDest(ctx, queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: data})
	return
}
