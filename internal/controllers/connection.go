package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/services"
	"net/http"
)

type ConnController struct{}

// CreateConnection	创建归档库连接信息
// @Router			/dest/conn [post]
// @Description		创建归档库连接信息
// @Tags			归档库连接信息
// @Param			connection		body		services.ConnService	true	"归档库连接信息"
// @Success			200				{object}	common.Response{data=services.ConnService}
// @Failure			500				{object}	common.Response
func (c *ConnController) CreateConnection(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)

	req := &services.ConnService{}
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

	res, err := req.CreateConn(ctx)
	if res.Code != common.CodeOK.Code {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: req})
}

// UpdateConnection	更新归档库连接信息
// @Router			/dest/conn [put]
// @Description		更新归档库连接信息
// @Tags			归档库连接信息
// @Param			Connection	body		services.ConnService	true	"Connection"
// @Success			200			{object}	common.Response{data=services.ConnService}
// @Failure			500			{object}	common.Response
func (c *ConnController) UpdateConnection(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)

	req := &services.ConnService{
		Model: services.Model{
			ID: common.InvalidUint,
		},
	}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr})
		return
	}

	if common.InvalidUintID(req.ID) {
		log.Errorf("invalid Connection.id(%d)", req.ID)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeInvalidID})
		return
	}

	res, err := req.UpdateConn(ctx)
	if res.Code != common.CodeOK.Code {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: req})
}

// DeleteConnection	删除归档库连接信息
// @Router			/dest/conn [delete]
// @Description		删除归档库连接信息
// @Tags			归档库连接信息
// @Param			Source	body		services.ConnService	true	"Source"
// @Success			200		{object}	common.Response
// @Failure			500		{object}	common.Response
func (c *ConnController) DeleteConnection(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)
	req := &services.ConnService{
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
		log.Errorf("validate conntction id(%d) or name(%s) not pass", req.ID, req.Name)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeConnParamErr})
		return
	}

	res, err := req.DeleteConn(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res})
	return
}

// QueryConnection  查询归档库连接信息
// @Router			/dest/conn [get]
// @Description		查询归档库连接信息
// @Tags			归档库连接信息
// @Param   		page			query		int			false  	"page"
// @Param   		pageSize		query		int     	false  	"pageSize"
// @Param   		id				query		uint     	false  	"连接ID"
// @Param   		creator			query		string     	false  	"创建人"
// @Param   		editor			query		string     	false  	"修改人"
// @Param   		name			query		string     	false  	"连接名字"
// @Param   		bu				query		string     	false  	"bu"
// @Param   		description		query		string     	false  	"说明"
// @Param   		storage			query		string     	false  	"存储介质"			Enums(mysql, databend)
// @Param   		data_bend_addr	query		string     	false  	"DataBend地址"
// @Param   		mysql_host		query		string     	false  	"mysql地址"
// @Success			200		{object}	common.Response{data=services.ConnService}
// @Failure			500		{object}	common.Response
func (c *ConnController) QueryConnection(ctx *gin.Context) {
	id := common.ParsingQueryUintID(ctx.Query("id"))
	queryMap := make(map[string]string, 10)
	queryMap["creator"] = ctx.Query("creator")
	queryMap["editor"] = ctx.Query("editor")
	queryMap["name"] = ctx.Query("name")
	queryMap["description"] = ctx.Query("description")
	queryMap["bu"] = ctx.Query("bu")
	queryMap["storage"] = ctx.Query("storage")
	queryMap["data_bend_addr"] = ctx.Query("data_bend_addr")
	queryMap["mysql_host"] = ctx.Query("mysql_host")

	source := services.ConnService{Model: services.Model{ID: id}}
	data, res, err := source.QueryConn(ctx, queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: data})
	return
}

// TestConnection	测试归档库连接信息
// @Router			/dest/conn/test [post]
// @Description		创建源端信息
// @Tags			归档库连接信息
// @Param			connection		body		services.ConnService	true	"归档库连接信息"
// @Success			200				{object}	common.Response{data=services.ConnService}
// @Failure			500				{object}	common.Response
func (c *ConnController) TestConnection(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)
	req := &services.ConnService{
		Model: services.Model{
			ID: common.InvalidUint,
		},
	}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr, Error: err.Error()})
		return
	}

	res, err := req.TestConn(ctx)
	if res.Code != common.CodeConnTestOK.Code {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res})
}
