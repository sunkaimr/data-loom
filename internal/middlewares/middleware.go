package middlewares

import (
	"embed"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"io/fs"
	"net/http"
)

func LoadMiddlewares(r *gin.Engine) *gin.Engine {
	r.Use(
		RecoverHandle(),
		Cors(),
		AddRequestID(),
		AddLogger(),
		Serve("/", EmbedFolder(f, "dist")),
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

//go:embed dist
var f embed.FS

func Serve(urlPrefix string, fs ServeFileSystem) gin.HandlerFunc {
	fileServer := http.FileServer(fs)
	if urlPrefix != "" {
		fileServer = http.StripPrefix(urlPrefix, fileServer)
	}
	return func(c *gin.Context) {
		if fs.Exists(urlPrefix, c.Request.URL.Path) {
			fileServer.ServeHTTP(c.Writer, c.Request)
			c.Abort()
		}
	}
}

type embedFileSystem struct {
	http.FileSystem
}

func (e embedFileSystem) Exists(_ string, path string) bool {
	_, err := e.Open(path)
	if err != nil {
		return false
	}
	return true
}

func EmbedFolder(fsEmbed embed.FS, targetPath string) ServeFileSystem {
	fSys, err := fs.Sub(fsEmbed, targetPath)
	if err != nil {
		panic(err)
	}
	return embedFileSystem{
		FileSystem: http.FS(fSys),
	}
}

type ServeFileSystem interface {
	http.FileSystem
	Exists(prefix string, path string) bool
}
