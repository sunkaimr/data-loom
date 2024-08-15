package middlewares

import (
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"net/http"
	"runtime/debug"
)

// RecoverHandle is a middleware to handle panic
func RecoverHandle() gin.HandlerFunc {
	return func(c *gin.Context) {
		log, _ := common.ExtractContext(c)
		defer func() {
			if err := recover(); err != nil {
				log.Error(err)
				debug.PrintStack()
				c.JSON(http.StatusInternalServerError, gin.H{
					"code":    http.StatusInternalServerError,
					"message": "Internal Server Error",
				})
				c.Abort()
			}
		}()
		c.Next()
	}
}
