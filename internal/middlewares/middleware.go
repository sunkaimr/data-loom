package middlewares

import (
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func LoadMiddlewares(r *gin.Engine) *gin.Engine {
	r.Use(
		RecoverHandle(),
		Cors(),
		AddRequestID(),
		AddLogger(),
	)
	return r
}

func NewGinContext(log *zap.SugaredLogger, db *gorm.DB) *gin.Context {
	ctx := &gin.Context{}
	ctx.Keys = make(map[string]any)
	ctx.Keys[common.LOGGER] = log
	ctx.Keys[common.DB] = db
	return ctx
}
