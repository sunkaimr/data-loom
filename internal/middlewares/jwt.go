package middlewares

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"net/http"
	"strings"
)

// Jwt middleware
func Jwt() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		log, _ := common.ExtractContext(ctx)
		tokenStr := ctx.GetHeader("Authorization")
		if tokenStr == "" {
			log.Errorf("Authorization empty")
			ctx.Abort()
			ctx.JSON(http.StatusUnauthorized, common.Response{ServiceCode: common.CodeMissAuth})
			return
		}
		parts := strings.Split(tokenStr, " ")

		if len(parts) != 2 || parts[0] != "Bearer" {
			log.Errorf("token[%s] format wrong", tokenStr)
			ctx.Abort()
			ctx.JSON(http.StatusUnauthorized, common.Response{ServiceCode: common.CodeMissAuth})
			return
		}

		token := parts[1]
		claims, res, err := common.JwtVerify(token)
		if err != nil {
			log.Errorf("JwtVerify failed, %s", err)
			ctx.Abort()
			ctx.JSON(http.StatusUnauthorized, common.Response{ServiceCode: res})
			return
		}

		ctx.Keys[common.JWT] = claims

		ctx.Next()
	}
}

// AdminVerify 校验是否是admin用户
func AdminVerify() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		log, _ := common.ExtractContext(ctx)
		u := common.ExtractUserInfo(ctx)
		if u == nil {
			err := fmt.Errorf("unknown user info, need '%s' user", common.AdminUser)
			log.Error(err)
			ctx.JSON(http.StatusUnauthorized, common.Response{ServiceCode: common.CodeDenied, Error: err.Error()})
			ctx.Abort()
			return
		}

		if u.UserName != common.AdminUser {
			log.Errorf("you(%s) access denied, only supports '%s'", u.UserName, common.AdminUser)
			ctx.JSON(http.StatusForbidden, common.Response{ServiceCode: common.CodeAdminOnly})
			ctx.Abort()
			return
		}

		ctx.Next()
	}
}
