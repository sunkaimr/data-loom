package middlewares

import (
	"bytes"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/pkg/logger"
	"github.com/sunkaimr/data-loom/internal/pkg/mysql"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"go.uber.org/zap"
	"io"
	"net/http"
	"strings"
	"time"
)

func AddRequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader(common.RequestID)
		if requestID == "" {
			requestID = utils.RandStr(20)
			c.Header(common.RequestID, requestID)
		}

		log := logger.AddFields(logger.Log, zap.String(common.RequestID, requestID))
		c.Set(common.LOGGER, log)
		c.Set(common.DB, (&mysql.GormLogger{Log: log}).WithLog())
		c.Next()
		return
	}
}

func AddLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		username := ""
		if jwt, ok := c.Keys[common.JWT].(*common.Claims); ok {
			username = jwt.UserName
		}

		log, _ := common.ExtractContext(c)

		if !strings.Contains(path, "/health") {
			query, body := c.Request.URL.RawQuery, ""
			if c.Request.ContentLength < 2048 {
				b, _ := io.ReadAll(c.Request.Body)
				c.Request.Body = io.NopCloser(bytes.NewBuffer(b))
				if strings.Contains(path, "/login") {
					b = []byte{}
				}
				body = string(b)
			}

			logger.AddFields(log,
				zap.String("process", "Request"),
				zap.String("user", username),
				zap.String("method", c.Request.Method),
				zap.String("url", c.Request.URL.Path),
				zap.String("remote_ip", c.RemoteIP()),
				zap.String("host", c.Request.Host),
				zap.Int64("bytes_in", c.Request.ContentLength),
				zap.String("protocol", c.Request.Proto)).
				Infof("query: %s, body: %s", query, body)
		}

		c.Next()

		if jwt, ok := c.Keys[common.JWT].(*common.Claims); ok {
			username = jwt.UserName
		}

		if strings.Contains(path, "/health") && c.Writer.Status() == http.StatusOK {
			return
		}

		logger.AddFields(log,
			zap.String("process", "Response"),
			zap.Int("status", c.Writer.Status()),
			zap.String("user", username),
			zap.String("method", c.Request.Method),
			zap.String("url", c.Request.URL.Path),
			zap.String("remote_ip", c.RemoteIP()),
			zap.String("host", c.Request.Host),
			zap.String("protocol", c.Request.Proto),
			zap.Int("bytes_out", c.Writer.Size()),
			zap.Duration("cost", time.Now().Sub(start))).
			Infof("")

		return
	}
}
