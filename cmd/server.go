package cmd

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/sunkaimr/data-loom/configs"
	"github.com/sunkaimr/data-loom/internal/job"
	"github.com/sunkaimr/data-loom/internal/middlewares"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/pkg/logger"
	"github.com/sunkaimr/data-loom/internal/pkg/mysql"
	"github.com/sunkaimr/data-loom/internal/router"
	"github.com/sunkaimr/data-loom/internal/services"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	config string
)

func NewServerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "server",
		Short: `Start the server`,
		Run: func(cmd *cobra.Command, args []string) {
			start()
		},
	}

	cmd.Flags().StringVarP(&config, "config", "", "./config.yaml", fmt.Sprintf("config file"))

	return cmd
}

func start() {
	// 读取配置
	configs.Init(config)
	logger.Init()

	// 初始化数据库
	if err := mysql.NewMysqlDB(&configs.C.Mysql); err != nil {
		panic(err)
	} else {
		models.UpdateModels()
	}

	log := logger.AddFields(logger.Log, zap.String(common.RequestID, time.Now().Format("20060102150405")+strings.Repeat("0", 6)))
	ctxCancel, cancel := context.WithCancel(context.TODO())
	db := (&mysql.GormLogger{Log: log}).WithLog()

	ctx := common.NewContext().WithContext(ctxCancel).WithCancel(cancel).WithLog(log).WithDB(db)
	SetupSignalHandler(ctx)

	services.Cfg.ReloadConfig(middlewares.NewGinContext(log, db))
	go startHttpServer(ctx)
	time.Sleep(time.Second * 1)

	go job.StartTaskJob(ctx)
	go job.StartOthersJob(ctx)

	time.Sleep(time.Second * 3)
	ctx.Wg.Wait()
	ctx.Log.Info("main exited")
}

func startHttpServer(ctx *common.Context) {
	server := &http.Server{
		Addr:    ":" + configs.C.Server.Port,
		Handler: router.Init(ctx),
	}
	ctx.Log.Infof("http server listen: %s", configs.C.Server.Port)
	go func() {
		ctx.Wg.Add(1)
		err := server.ListenAndServe()
		if err != nil {
			if errors.Is(err, http.ErrServerClosed) {
				ctx.Log.Infof("http server closed")
			} else {
				ctx.Log.Fatalf("server listen error: %s", err)
			}
		}
		ctx.Wg.Done()
	}()

	<-ctx.Context.Done()
	ctx.Log.Infof("shutdown http server...")
	err := server.Shutdown(ctx.Context)
	if err != nil {
		ctx.Log.Errorf("http server shutdown failed, err:%s", err)
	}
}

func SetupSignalHandler(ctx *common.Context) {
	c := make(chan os.Signal, 2)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-c
		ctx.Log.Infof("received signal: %s, exiting...", sig)
		ctx.Cancel()
		<-c
		ctx.Log.Infof("received signal: %s, force exited", sig)
		os.Exit(1)
	}()
}
