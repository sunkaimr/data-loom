package controllers

import (
	"github.com/sunkaimr/data-loom/internal/notice"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/services"
	"net/http"

	"github.com/gin-gonic/gin"
)

type ConfigController struct{}

// GetConfig		查询配置
// @Router			/manage/config [get]
// @Description		查询配置
// @Tags			管理员
// @Success			200		{object}	common.Response{data=services.ConfigService}
// @Failure			500		{object}	common.Response
func (c *ConfigController) GetConfig(ctx *gin.Context) {
	cfg, res, err := new(services.ConfigService).GetConfig(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: cfg})
}

// UpdateConfig		修改用户配置
// @Router			/manage/config [put]
// @Description		修改用户配置
// @Tags			管理员
// @Param			config	body		services.ConfigService	true 	"config"
// @Success			200		{object}	common.Response{data=services.ConfigService}
// @Failure			500		{object}	common.Response
func (c *ConfigController) UpdateConfig(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)
	req := &services.ConfigService{}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr})
		return
	}

	cfg, res, err := req.UpdateConfig(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: cfg})
}

// NoticeTest		通知测试
// @Router			/notice/test [get]
// @Description		通知测试
// @Tags			管理员
// @Param   		user				query		string			false  	"user"
// @Success			200		{object}	common.Response
// @Failure			500		{object}	common.Response
func (c *ConfigController) NoticeTest(ctx *gin.Context) {
	user := ctx.Query("user")
	if user == "" {
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeConfigNoticeUserErr})
		return
	}

	(&services.ConfigService{}).ReloadConfig(ctx)

	err := notice.NewDriver(services.Cfg.ServiceToModel()).Test(nil, user)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeConfigNoticeErr, Error: err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, common.Response{ServiceCode: common.CodeConfigNoticeOK})
	return
}
