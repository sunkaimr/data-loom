package router

import (
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/configs"
	doc "github.com/sunkaimr/data-loom/docs"
	ctl "github.com/sunkaimr/data-loom/internal/controllers"
	"github.com/sunkaimr/data-loom/internal/middlewares"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	swgfiles "github.com/swaggo/files"
	ginswg "github.com/swaggo/gin-swagger"
	"github.com/swaggo/swag"
	"io"
	"net/http"
	"net/url"
)

var Router *gin.Engine

func Init(ctx *common.Context) *gin.Engine {
	gin.DefaultWriter = io.Discard
	Router = gin.Default()

	middlewares.LoadMiddlewares(Router)

	// 公共路由无需鉴权
	public := Router.Group("/data-loom/api/v1")
	{
		public.GET("/health", new(ctl.HealthController).Health)
	}

	// 用户相关路由
	user := public.Group("/user")
	{
		// 用户登录
		user.PUT("/login", new(ctl.UserController).Login)
		// 修改用户信息
		user.PUT("/", middlewares.Jwt(), new(ctl.UserController).ModifyUser)
	}

	// 集群信息相关路由
	clu := public.Group("/cluster", middlewares.Jwt())
	{
		// 创建集群
		clu.POST("/", middlewares.AdminVerify(), new(ctl.ClusterController).CreateCluster)
		// 修改集群信息
		clu.PUT("/", middlewares.AdminVerify(), new(ctl.ClusterController).UpdateCluster)
		// 查询用户有权限操作的集群
		clu.GET("/", new(ctl.ClusterController).QueryCluster)
		// 删除集群
		clu.DELETE("/", middlewares.AdminVerify(), new(ctl.ClusterController).DeleteCluster)

		// 同步集群信息
		clu.GET("/sync", middlewares.AdminVerify(), new(ctl.ClusterController).SyncCluster)
		// 收集集群大表信息
		clu.GET("/bigtable", middlewares.AdminVerify(), new(ctl.ClusterController).GetClusterBigTables)

		// 查询集群对应的库列表
		clu.GET("/:cluster_id/databases", new(ctl.ClusterController).GetClusterDatabases)
		// 查询集群对应的表列表
		clu.GET("/:cluster_id/:database/tables", new(ctl.ClusterController).GetClusterTables)

		// 日期维度统计信息
		clu.GET("/statistic/date", new(ctl.ClusterStatisticController).ClusterStatisticGroupByDate)
		// BU维度统计信息
		clu.GET("/statistic/bu", new(ctl.ClusterStatisticController).ClusterStatisticGroupByBu)
		// 集群维度统计信息
		clu.GET("/statistic/cluster", new(ctl.ClusterStatisticController).ClusterStatisticGroupByCluster)
		// 库维度统计信息
		clu.GET("/statistic/database", new(ctl.ClusterStatisticController).ClusterStatisticGroupByDatabase)
		// 表维度统计信息
		clu.GET("/statistic/table", new(ctl.ClusterStatisticController).ClusterStatisticGroupByTable)
	}

	// 源端信息相关路由
	source := public.Group("/source", middlewares.Jwt())
	{
		// 创建源端信息
		source.POST("/", new(ctl.SourceController).CreateSource)
		// 修改源端信息
		source.PUT("/", new(ctl.SourceController).UpdateSource)
		// 查询源端信息
		source.GET("/", new(ctl.SourceController).QuerySource)
		// 删除源端信息
		source.DELETE("/", new(ctl.SourceController).DeleteSource)
	}

	// 目标端信息相关路由
	dest := public.Group("/dest", middlewares.Jwt())
	{
		// 创建目标端信息
		dest.POST("/", middlewares.AdminVerify(), new(ctl.DestController).CreateDestination)
		// 修改目标端信息
		dest.PUT("/", middlewares.AdminVerify(), new(ctl.DestController).UpdateDestination)
		// 查询目标端信息
		dest.GET("/", new(ctl.DestController).QueryDestination)
		// 删除目标端信息
		dest.DELETE("/", middlewares.AdminVerify(), new(ctl.DestController).DeleteDestination)

		conn := dest.Group("/conn")
		{
			// 查询连接信息
			conn.GET("/", new(ctl.ConnController).QueryConnection)
			// 创建连接信息
			conn.POST("/", middlewares.AdminVerify(), new(ctl.ConnController).CreateConnection)
			// 修改连接信息
			conn.PUT("/", middlewares.AdminVerify(), new(ctl.ConnController).UpdateConnection)
			// 删除连接信息
			conn.DELETE("/", middlewares.AdminVerify(), new(ctl.ConnController).DeleteConnection)
			// 测试连接信息
			conn.POST("/test", new(ctl.ConnController).TestConnection)
		}
	}

	// 策略相关路由
	policy := public.Group("/policy", middlewares.Jwt())
	{
		// 创建策略
		policy.POST("/", new(ctl.PolicyController).CreatePolicy)
		// 修改策略
		policy.PUT("/", new(ctl.PolicyController).UpdatePolicy)
		// 查询策略
		policy.GET("/", new(ctl.PolicyController).QueryPolicy)
		// 删除策略
		policy.DELETE("/", new(ctl.PolicyController).DeletePolicy)
		// 查询策略修订记录
		policy.GET("/revision", new(ctl.PolicyRevisionController).QueryPolicyRevision)
		// BU维度统计信息
		policy.GET("/statistic/bu", new(ctl.PolicyStatisticController).PolicyStatisticGroupByBu)
		// 集群维度统计信息
		policy.GET("/statistic/cluster", new(ctl.PolicyStatisticController).PolicyStatisticGroupByCluster)
		// 库维度统计信息
		policy.GET("/statistic/database", new(ctl.PolicyStatisticController).PolicyStatisticGroupByDatabase)
		// 表维度统计信息
		policy.GET("/statistic/table", new(ctl.PolicyStatisticController).PolicyStatisticGroupByTable)
	}

	// 任务相关路由
	task := public.Group("/task", middlewares.Jwt())
	{
		// 修改任务
		task.PUT("/", new(ctl.TaskController).UpdateTask)
		// 查询任务
		task.GET("/", new(ctl.TaskController).QueryTask)
		// 删除任务
		task.DELETE("/", new(ctl.TaskController).DeleteTask)
		// 查询任务修订记录
		task.GET("/revision", new(ctl.TaskRevisionController).QueryPolicyRevision)
		// 查询任务修订记录
		task.GET("/changelog", new(ctl.TaskChangeLogController).QueryTaskChangeLog)
		// 上报任务执行结果
		task.PUT("/result", new(ctl.TaskController).UpdateTaskResult)
		// 查询任务状态统计
		task.GET("/statistic/summary", new(ctl.TaskStatisticController).TaskStatisticSummary)
		// BU维度统计信息
		task.GET("/statistic/bu", new(ctl.TaskStatisticController).TaskStatisticGroupByBu)
		// 集群维度统计信息
		task.GET("/statistic/cluster", new(ctl.TaskStatisticController).TaskStatisticGroupByCluster)
		// 库维度统计信息
		task.GET("/statistic/database", new(ctl.TaskStatisticController).TaskStatisticGroupByDatabase)
		// 表维度统计信息
		task.GET("/statistic/table", new(ctl.TaskStatisticController).TaskStatisticGroupByTable)
		// 任务的执行计划
		task.GET("/plan", new(ctl.TaskStatisticController).TaskExecPlan)
	}

	// 管理员相关路由
	manage := public.Group("/manage", middlewares.Jwt(), middlewares.AdminVerify())
	{
		// 注册用户
		manage.POST("/user/register", new(ctl.UserController).RegisterUser)
		// 删除用户
		manage.DELETE("/user/:user", new(ctl.UserController).DeleteUser)
		// 查询用户
		manage.GET("/user", new(ctl.UserController).QueryUser)

		// 查询配置
		manage.GET("/config", new(ctl.ConfigController).GetConfig)
		// 更新配置
		manage.PUT("/config", new(ctl.ConfigController).UpdateConfig)
		// 通知测试
		manage.GET("/notice/test", new(ctl.ConfigController).NoticeTest)
	}

	// init swagger
	if configs.C.Server.ExternalAddr != "" {
		sw := swag.GetSwagger(doc.SwaggerInfo.InfoInstanceName).(*swag.Spec)
		u, err := url.Parse(configs.C.Server.ExternalAddr)
		if err != nil {
			ctx.Log.Fatalf("config: server.externalAddr parse failed, %s", err)
		}
		sw.Schemes = []string{u.Scheme}
		sw.Host = u.Host
	}
	Router.GET("/data-loom/api/v1/swagger/*any", ginswg.WrapHandler(swgfiles.Handler))
	Router.NoRoute(func(ctx *gin.Context) {
		ctx.JSON(http.StatusNotFound, common.Response{ServiceCode: common.CodeNotFound})
	})

	return Router
}
