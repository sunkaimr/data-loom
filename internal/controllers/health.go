package controllers

import (
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"net/http"

	"github.com/gin-gonic/gin"
)

type HealthController struct{}

// Health			健康检查
// @Router			/health [get]
// @Description		健康检查
// @Tags			其他
// @Success			200		{object}	common.Response
// @Failure			500		{object}	common.Response
func (c *HealthController) Health(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, common.Response{ServiceCode: common.CodeOK})
}
