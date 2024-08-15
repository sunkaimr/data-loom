package common

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/sunkaimr/data-loom/configs"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"strings"
	"time"
)

type Claims struct {
	jwt.RegisteredClaims

	UserID   uint   `json:"userid"`
	UserName string `json:"username"`
	RealName string `json:"real_name"`
}

func GenerateToken(claims *Claims) (string, error) {
	secret := configs.C.Jwt.Secret

	if claims.ExpiresAt == nil {
		claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Hour * 24))
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		return "", err
	}
	return token, nil
}

func JwtVerify(tokenStr string) (*Claims, ServiceCode, error) {
	secret := configs.C.Jwt.Secret
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(secret), nil
	})

	switch {
	case err == nil:
	case strings.Contains(err.Error(), jwt.ErrTokenExpired.Error()):
		return nil, CodeTokenExpired, fmt.Errorf("token expired")
	default:
		return nil, CodeTokenErr, fmt.Errorf("token invalid")
	}

	if !token.Valid {
		return nil, CodeTokenErr, fmt.Errorf("token invalid")
	}

	claims, ok := token.Claims.(*Claims)
	if !ok {
		return nil, CodeTokenErr, fmt.Errorf("token.Claims invalid")
	}
	if float64(claims.ExpiresAt.Unix()) < float64(time.Now().Unix()) {
		return nil, CodeTokenExpired, fmt.Errorf("token expired")
	}

	return claims, CodeOK, nil

}

func ExtractUserInfo(c *gin.Context) *Claims {
	if v, ok := c.Keys[JWT].(*Claims); ok {
		return v
	}
	return nil
}

func ExtractContext(c *gin.Context) (log *zap.SugaredLogger, db *gorm.DB) {
	if v, ok := c.Keys[LOGGER].(*zap.SugaredLogger); ok {
		log = v
	}
	if v, ok := c.Keys[DB].(*gorm.DB); ok {
		db = v
	}
	return log, db
}
