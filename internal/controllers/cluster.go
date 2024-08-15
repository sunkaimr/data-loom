package controllers

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/services"
	"net/http"
)

type ClusterController struct{}

// CreateCluster     创建集群信息
// @Router			/cluster [post]
// @Description		创建源端信息
// @Tags			集群管理
// @Param			Cluster	body		services.ClusterService	true	"Cluster"
// @Success			200		{object}	common.Response{data=services.ClusterService}
// @Failure			500		{object}	common.Response
func (c *ClusterController) CreateCluster(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)

	req := &services.ClusterService{}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr, Error: err.Error()})
		return
	}

	// 参数校验
	if ok, res, err := req.CheckParameters(ctx); !ok {
		log.Errorf("check parameters(%+v) not pass, %s", req, err)
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}

	res, err := req.CreateCluster(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: req})
	return
}

// UpdateCluster     更新集群信息
// @Router			/cluster [put]
// @Description		更新集群信息
// @Tags			集群管理
// @Param			Cluster	body		services.ClusterService	true	"Cluster"
// @Success			200		{object}	common.Response{data=services.ClusterService}
// @Failure			500		{object}	common.Response
func (c *ClusterController) UpdateCluster(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)

	req := &services.ClusterService{Model: services.Model{ID: common.InvalidUint}}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr})
		return
	}

	ok, res, err := req.CheckUpdateParameters(ctx)
	if !ok {
		log.Errorf("check update parameters no paas, %s", err)
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}

	code, err := req.UpdateCluster(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: req})
	return
}

// DeleteCluster     删除集群信息
// @Router			/cluster [delete]
// @Description		删除集群信息
// @Tags			集群管理
// @Param			Cluster	body		services.ClusterService	true	"Cluster"
// @Success			200		{object}	common.Response
// @Failure			500		{object}	common.Response
func (c *ClusterController) DeleteCluster(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)
	req := &services.ClusterService{Model: services.Model{ID: common.InvalidUint}}
	if err := ctx.ShouldBindJSON(req); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeBindErr})
		return
	}
	// 参数校验
	if common.InvalidUintID(req.ID) && len(req.ClusterID) == 0 && len(req.ClusterName) == 0 {
		log.Errorf("validate cluster id(%d) or cluster_name(%s) or cluster_id(%s) not pass", req.ID, req.ClusterName, req.ClusterID)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeClusterNameAndIDErr})
		return
	}

	code, err := req.DeleteCluster(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code})
	return
}

// QueryCluster      查询集群信息
// @Router			/cluster [get]
// @Description		查询集群信息
// @Tags			集群管理
// @Param   		page			query		int			false  	"page"
// @Param   		pageSize		query		int     	false  	"pageSize"
// @Param   		id				query		uint     	false  	"ID"
// @Param   		creator			query		string     	false  	"创建人"
// @Param   		editor			query		string     	false  	"修改人"
// @Param   		cluster_id		query		string     	false  	"cluster_id"
// @Param   		cluster_name	query		string     	false  	"集群名称"
// @Param   		description		query		string     	false  	"说明"
// @Param   		bu				query		string     	false  	"bu"
// @Param   		env				query		string     	false  	"env"
// @Param   		import_from  	query		string     	false  	"添加方式：customized:自定义添加 ipaas:从资源中心导入"
// @Param   		cluster_type	query		string     	false  	"集群类型（mysql, 其他的）"
// @Param   		service_addr	query		string     	false  	"集群访问地址"
// @Param   		write_addr  	query		string     	false  	"主库地址"
// @Param   		read_addr		query		string     	false  	"从库地址"
// @Param   		user_name		query		string     	false  	"用户名"
// @Success			200		{object}	common.Response{data=services.ClusterService}
// @Failure			500		{object}	common.Response
func (c *ClusterController) QueryCluster(ctx *gin.Context) {
	id := common.ParsingQueryUintID(ctx.Query("id"))
	queryMap := make(map[string]string, 10)
	queryMap["creator"] = ctx.Query("creator")
	queryMap["editor"] = ctx.Query("editor")
	queryMap["cluster_id"] = ctx.Query("cluster_id")
	queryMap["cluster_name"] = ctx.Query("cluster_name")
	queryMap["description"] = ctx.Query("description")
	queryMap["bu"] = ctx.Query("bu")
	queryMap["env"] = ctx.Query("env")
	queryMap["import_from"] = ctx.Query("import_from")
	queryMap["cluster_type"] = ctx.Query("cluster_type")
	queryMap["service_addr"] = ctx.Query("service_addr")
	queryMap["write_addr"] = ctx.Query("write_addr")
	queryMap["read_addr"] = ctx.Query("read_addr")
	queryMap["user_name"] = ctx.Query("user_name")

	queryCluster := services.ClusterService{Model: services.Model{ID: id}}
	data, res, err := queryCluster.QueryCluster(ctx, queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(res), common.Response{ServiceCode: res, Data: data})
	return
}

// GetClusterDatabases 查询指定数据库集群的库列表
// @Router			/cluster/{cluster_id}/databases [get]
// @Description		查询指定数据库集群的库列表
// @Tags			集群管理
// @Param  			cluster_id    path  string  true  "cluster id"
// @Success			200			{object}	common.Response{data=[]string}
// @Failure			500			{object}	common.Response
func (c *ClusterController) GetClusterDatabases(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)

	clusterID := ctx.Param("cluster_id")
	if clusterID == "" {
		log.Errorf("cluster_id(%s) empty", clusterID)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeClusterIDEmpty})
		return
	}

	// TODO 校验用户对集群的操作权限

	clusterSvc := &services.ClusterService{ClusterID: clusterID}
	databases, code, err := clusterSvc.GetClusterDatabase(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: databases})
	return
}

// GetClusterTables 查询指定数据库集群的库列表
// @Router			/cluster/{cluster_id}/{database}/tables [get]
// @Description		查询指定数据库集群的库列表
// @Tags			集群管理
// @Param  			cluster_id 	path  string  true  "cluster id"
// @Param  			database    path  string  true  "database name"
// @Success			200			{object}	common.Response{data=[]string}
// @Failure			500			{object}	common.Response
func (c *ClusterController) GetClusterTables(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)

	clusterID := ctx.Param("cluster_id")
	if clusterID == "" {
		log.Errorf("cluster_id(%s) empty", clusterID)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeClusterIDEmpty})
		return
	}

	database := ctx.Param("database")
	if database == "" {
		log.Errorf("database(%s) empty", database)
		ctx.JSON(http.StatusBadRequest, common.Response{ServiceCode: common.CodeClusterDatabaseEmpty})
		return
	}

	// TODO 校验用户对集群的操作权限

	clusterSvc := &services.ClusterService{ClusterID: clusterID}
	tables, code, err := clusterSvc.GetClusterTables(ctx, database)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: tables})
	return
}

// SyncCluster     同步集群信息
// @Router			/cluster/sync [get]
// @Description		同步集群信息
// @Tags			集群管理
// @Success			200		{object}	common.Response
// @Failure			500		{object}	common.Response
func (c *ClusterController) SyncCluster(ctx *gin.Context) {
	cluster := &services.ClusterService{}
	code, err := cluster.SyncCluster(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code})
	return
}

// GetClusterBigTables  获取集群的大表信息（默认只获取10G以上的大表）
// @Router				/cluster/bigtable [get]
// @Description			获取集群的大表信息（默认只获取10G以上的大表）
// @Tags				集群管理
// @Success			200		{object}	common.Response
// @Failure			500		{object}	common.Response
func (c *ClusterController) GetClusterBigTables(ctx *gin.Context) {
	if !services.RefreshClusterBigTablesLock.TryLock() {
		err := fmt.Errorf("there is already a task collecting information about the large table of the cluster")
		ctx.JSON(common.ServiceCode2HttpCode(common.CodeClusterCollectedTaskExisted), common.Response{ServiceCode: common.CodeClusterCollectedTaskExisted, Error: err.Error()})
		return
	}
	services.RefreshClusterBigTablesLock.Unlock()

	go func() {
		_, _ = (&services.ClusterService{}).RefreshClusterBigTables(ctx)
	}()

	ctx.JSON(common.ServiceCode2HttpCode(common.CodeClusterCollectedTaskRunning), common.Response{ServiceCode: common.CodeClusterCollectedTaskRunning})
	return
}
