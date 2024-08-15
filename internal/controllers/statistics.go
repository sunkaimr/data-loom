package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/services"
)

type TaskStatisticController struct{}
type PolicyStatisticController struct{}
type ClusterStatisticController struct{}

// TaskStatisticSummary	查询任务统计信息
// @Router				/task/statistic/summary [get]
// @Description			查询任务统计信息
// @Tags				任务统计
// @Param   			start_date			query		string     	false  	"统计开始日期"
// @Param   			end_date			query		string     	false  	"统计结束日期"
// @Success				200		{object}	common.Response{data=services.TaskStatisticService}
// @Failure				500		{object}	common.Response
func (c *TaskStatisticController) TaskStatisticSummary(ctx *gin.Context) {
	svc := services.TaskStatisticService{
		StartDate: ctx.Query("start_date"),
		EndDate:   ctx.Query("end_date"),
	}
	data, code, err := svc.TaskStatisticSummary(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// TaskExecPlan			查询任务执行计划
// @Router				/task/plan [get]
// @Description			查询任务执行计划
// @Tags				任务统计
// @Param   			start_date			query		string     	false  	"统计开始日期"
// @Param   			end_date			query		string     	false  	"统计结束日期"
// @Success				200		{object}	common.Response{data=services.TaskPlanService}
// @Failure				500		{object}	common.Response
func (c *TaskStatisticController) TaskExecPlan(ctx *gin.Context) {
	svc := services.TaskPlanService{
		StartDate: ctx.Query("start_date"),
		EndDate:   ctx.Query("end_date"),
	}
	data, code, err := svc.TaskExecPlan(ctx)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// TaskStatisticGroupByTable	查询表维度统计信息
// @Router						/task/statistic/table [get]
// @Description					查询表维度统计信息
// @Tags						任务统计
// @Param   					start_date		query		string     	false  	"统计开始日期"
// @Param   					end_date		query		string     	false  	"统计结束日期"
// @Param   					bu				query		string     	false  	"BU"
// @Param   					cluster_name	query		string     	false  	"集群名称"
// @Param   					database		query		string     	false  	"库名"
// @Param   					table			query		string     	false  	"表名"
// @Param   					govern			query		string     	false  	"治理方式"
// @Success						200		{object}	common.Response{data=services.TaskStatisticDetail}
// @Failure						500		{object}	common.Response
func (c *TaskStatisticController) TaskStatisticGroupByTable(ctx *gin.Context) {
	queryMap := make(map[string]string, 10)
	queryMap["src_bu"] = ctx.Query("bu")
	queryMap["src_cluster_name"] = ctx.Query("cluster_name")
	queryMap["src_database_name"] = ctx.Query("database")
	queryMap["src_tables_name"] = ctx.Query("table")
	queryMap["govern"] = ctx.Query("govern")

	svc := services.TaskStatisticDetail{
		StartDate: ctx.Query("start_date"),
		EndDate:   ctx.Query("end_date"),
	}
	data, code, err := svc.TaskStatisticDetail(ctx, "table", queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// TaskStatisticGroupByDatabase	查询库维度统计信息
// @Router						/task/statistic/database [get]
// @Description					查询库维度统计信息
// @Tags						任务统计
// @Param   					start_date		query		string     	false  	"统计开始日期"
// @Param   					end_date		query		string     	false  	"统计结束日期"
// @Param   					bu				query		string     	false  	"BU"
// @Param   					cluster_name	query		string     	false  	"集群名称"
// @Param   					database		query		string     	false  	"库名"
// @Param   					govern			query		string     	false  	"治理方式"
// @Success						200		{object}	common.Response{data=services.TaskStatisticDetail}
// @Failure						500		{object}	common.Response
func (c *TaskStatisticController) TaskStatisticGroupByDatabase(ctx *gin.Context) {
	queryMap := make(map[string]string, 10)
	queryMap["src_bu"] = ctx.Query("bu")
	queryMap["src_cluster_name"] = ctx.Query("cluster_name")
	queryMap["src_database_name"] = ctx.Query("database")
	queryMap["govern"] = ctx.Query("govern")

	svc := services.TaskStatisticDetail{
		StartDate: ctx.Query("start_date"),
		EndDate:   ctx.Query("end_date"),
	}
	data, code, err := svc.TaskStatisticDetail(ctx, "database", queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// TaskStatisticGroupByCluster	查询集群维度统计信息
// @Router						/task/statistic/cluster [get]
// @Description					查询集群维度统计信息
// @Tags						任务统计
// @Param   					start_date		query		string     	false  	"统计开始日期"
// @Param   					end_date		query		string     	false  	"统计结束日期"
// @Param   					bu				query		string     	false  	"BU"
// @Param   					cluster_name	query		string     	false  	"集群名称"
// @Param   					govern			query		string     	false  	"治理方式"
// @Success						200		{object}	common.Response{data=services.TaskStatisticDetail}
// @Failure						500		{object}	common.Response
func (c *TaskStatisticController) TaskStatisticGroupByCluster(ctx *gin.Context) {
	queryMap := make(map[string]string, 10)
	queryMap["src_bu"] = ctx.Query("bu")
	queryMap["src_cluster_name"] = ctx.Query("cluster_name")
	queryMap["govern"] = ctx.Query("govern")

	svc := services.TaskStatisticDetail{
		StartDate: ctx.Query("start_date"),
		EndDate:   ctx.Query("end_date"),
	}
	data, code, err := svc.TaskStatisticDetail(ctx, "cluster", queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// TaskStatisticGroupByBu		查询bu维度统计信息
// @Router						/task/statistic/bu [get]
// @Description					查询bu维度统计信息
// @Tags						任务统计
// @Param   					start_date		query		string     	false  	"统计开始日期"
// @Param   					end_date		query		string     	false  	"统计结束日期"
// @Param   					bu				query		string     	false  	"BU"
// @Param   					govern			query		string     	false  	"治理方式"
// @Success						200		{object}	common.Response{data=services.TaskStatisticDetail}
// @Failure						500		{object}	common.Response
func (c *TaskStatisticController) TaskStatisticGroupByBu(ctx *gin.Context) {
	queryMap := make(map[string]string, 10)
	queryMap["src_bu"] = ctx.Query("bu")
	queryMap["govern"] = ctx.Query("govern")

	svc := services.TaskStatisticDetail{
		StartDate: ctx.Query("start_date"),
		EndDate:   ctx.Query("end_date"),
	}
	data, code, err := svc.TaskStatisticDetail(ctx, "bu", queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// PolicyStatisticGroupByTable	查询表维度统计信息
// @Router						/policy/statistic/table [get]
// @Description					查询表维度统计信息
// @Tags						任务统计
// @Param   					enable			query		bool     	false  	"是否开启"
// @Param   					bu				query		string     	false  	"BU"
// @Param   					cluster_name	query		string     	false  	"集群名称"
// @Param   					database		query		string     	false  	"库名"
// @Param   					table			query		string     	false  	"表名"
// @Param   					govern			query		string     	false  	"治理方式"
// @Success						200		{object}	common.Response{data=services.PolicyStatistic}
// @Failure						500		{object}	common.Response
func (c *PolicyStatisticController) PolicyStatisticGroupByTable(ctx *gin.Context) {
	queryMap := make(map[string]string, 10)
	queryMap["source.bu"] = ctx.Query("bu")
	queryMap["source.cluster_name"] = ctx.Query("cluster_name")
	queryMap["source.database_name"] = ctx.Query("database")
	queryMap["source.tables_name"] = ctx.Query("table")
	queryMap["policy.govern"] = ctx.Query("govern")

	svc := services.PolicyStatistic{}
	data, code, err := svc.PolicyStatisticDetail(ctx, "table", queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// PolicyStatisticGroupByDatabase	查询库维度统计信息
// @Router						/policy/statistic/database [get]
// @Description					查询库维度统计信息
// @Tags						任务统计
// @Param   					enable			query		bool     	false  	"是否开启"
// @Param   					bu				query		string     	false  	"BU"
// @Param   					cluster_name	query		string     	false  	"集群名称"
// @Param   					database		query		string     	false  	"库名"
// @Param   					govern			query		string     	false  	"治理方式"
// @Success						200		{object}	common.Response{data=services.PolicyStatistic}
// @Failure						500		{object}	common.Response
func (c *PolicyStatisticController) PolicyStatisticGroupByDatabase(ctx *gin.Context) {
	queryMap := make(map[string]string, 10)
	queryMap["source.bu"] = ctx.Query("bu")
	queryMap["source.cluster_name"] = ctx.Query("cluster_name")
	queryMap["source.database_name"] = ctx.Query("database")
	queryMap["policy.govern"] = ctx.Query("govern")

	svc := services.PolicyStatistic{}
	data, code, err := svc.PolicyStatisticDetail(ctx, "database", queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// PolicyStatisticGroupByCluster	查询集群维度统计信息
// @Router						/policy/statistic/cluster [get]
// @Description					查询集群维度统计信息
// @Tags						任务统计
// @Param   					enable			query		bool     	false  	"是否开启"
// @Param   					bu				query		string     	false  	"BU"
// @Param   					cluster_name	query		string     	false  	"集群名称"
// @Param   					govern			query		string     	false  	"治理方式"
// @Success						200		{object}	common.Response{data=services.PolicyStatistic}
// @Failure						500		{object}	common.Response
func (c *PolicyStatisticController) PolicyStatisticGroupByCluster(ctx *gin.Context) {
	queryMap := make(map[string]string, 10)
	queryMap["source.bu"] = ctx.Query("bu")
	queryMap["source.cluster_name"] = ctx.Query("cluster_name")
	queryMap["policy.govern"] = ctx.Query("govern")

	svc := services.PolicyStatistic{}
	data, code, err := svc.PolicyStatisticDetail(ctx, "cluster", queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// PolicyStatisticGroupByBu		查询bu维度统计信息
// @Router						/policy/statistic/bu [get]
// @Description					查询bu维度统计信息
// @Tags						任务统计
// @Param   					enable			query		bool     	false  	"是否开启"
// @Param   					bu				query		string     	false  	"BU"
// @Param   					govern			query		string     	false  	"治理方式"
// @Success						200		{object}	common.Response{data=services.PolicyStatistic}
// @Failure						500		{object}	common.Response
func (c *PolicyStatisticController) PolicyStatisticGroupByBu(ctx *gin.Context) {
	queryMap := make(map[string]string, 10)
	queryMap["source.bu"] = ctx.Query("bu")
	queryMap["policy.govern"] = ctx.Query("govern")

	svc := services.PolicyStatistic{}
	data, code, err := svc.PolicyStatisticDetail(ctx, "bu", queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// ClusterStatisticGroupByTable	查询表维度集群的大表统计信息
// @Router						/cluster/statistic/table [get]
// @Description					查询表维度集群的大表统计信息
// @Tags						统计信息
// @Param   					start_date		query		string     	false  	"统计开始日期"
// @Param   					end_date		query		string     	false  	"统计结束日期"
// @Param   					start_size		query		string     	false  	"统计大于等于该值的表"
// @Param   					end_size		query		string     	false  	"统计小于等于该值的表"
// @Param   					bu				query		string     	false  	"BU"
// @Param   					cluster_id		query		string     	false  	"集群ID"
// @Param   					cluster_name	query		string     	false  	"集群名称"
// @Param   					database		query		string     	false  	"库名"
// @Param   					table			query		string     	false  	"表名"
// @Success						200				{object}	common.Response{data=services.ClusterStatistic}
// @Failure						500				{object}	common.Response
func (c *ClusterStatisticController) ClusterStatisticGroupByTable(ctx *gin.Context) {
	queryMap := make(map[string]string, 10)
	queryMap["bu"] = ctx.Query("bu")
	queryMap["cluster_id"] = ctx.Query("cluster_id")
	queryMap["cluster_name"] = ctx.Query("cluster_name")
	queryMap["`database`"] = ctx.Query("database")
	queryMap["`table`"] = ctx.Query("table")

	svc := services.ClusterStatistic{
		StartDate: ctx.Query("start_date"),
		EndDate:   ctx.Query("end_date"),
		StartSize: ctx.Query("start_size"),
		EndSize:   ctx.Query("end_size"),
	}
	data, code, err := svc.ClusterStatisticDetail(ctx, "table", queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// ClusterStatisticGroupByDatabase	查询库维度集群的大表统计信息
// @Router							/cluster/statistic/database [get]
// @Description						查询库维度集群的大表统计信息
// @Tags							统计信息
// @Param   						start_date		query		string     	false  	"统计开始日期"
// @Param   						end_date		query		string     	false  	"统计结束日期"
// @Param   						start_size		query		string     	false  	"统计大于等于该值的表"
// @Param   						end_size		query		string     	false  	"统计小于等于该值的表"
// @Param   						bu				query		string     	false  	"BU"
// @Param   						cluster_id		query		string     	false  	"集群ID"
// @Param   						cluster_name	query		string     	false  	"集群名称"
// @Param   						database		query		string     	false  	"库名"
// @Success							200				{object}	common.Response{data=services.ClusterStatistic}
// @Failure							500				{object}	common.Response
func (c *ClusterStatisticController) ClusterStatisticGroupByDatabase(ctx *gin.Context) {
	queryMap := make(map[string]string, 10)
	queryMap["bu"] = ctx.Query("bu")
	queryMap["cluster_id"] = ctx.Query("cluster_id")
	queryMap["cluster_name"] = ctx.Query("cluster_name")
	queryMap["`database`"] = ctx.Query("database")

	svc := services.ClusterStatistic{
		StartDate: ctx.Query("start_date"),
		EndDate:   ctx.Query("end_date"),
		StartSize: ctx.Query("start_size"),
		EndSize:   ctx.Query("end_size"),
	}
	data, code, err := svc.ClusterStatisticDetail(ctx, "database", queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// ClusterStatisticGroupByCluster	查询集群维度集群的大表统计信息
// @Router							/cluster/statistic/cluster [get]
// @Description						查询集群维度集群的大表统计信息
// @Tags							统计信息
// @Param   						start_date		query		string     	false  	"统计开始日期"
// @Param   						end_date		query		string     	false  	"统计结束日期"
// @Param   						start_size		query		string     	false  	"统计大于等于该值的表"
// @Param   						end_size		query		string     	false  	"统计小于等于该值的表"
// @Param   						bu				query		string     	false  	"BU"
// @Param   						cluster_id		query		string     	false  	"集群ID"
// @Param   						cluster_name	query		string     	false  	"集群名称"
// @Success							200				{object}	common.Response{data=services.ClusterStatistic}
// @Failure							500				{object}	common.Response
func (c *ClusterStatisticController) ClusterStatisticGroupByCluster(ctx *gin.Context) {
	queryMap := make(map[string]string, 10)
	queryMap["bu"] = ctx.Query("bu")
	queryMap["cluster_id"] = ctx.Query("cluster_id")
	queryMap["cluster_name"] = ctx.Query("cluster_name")

	svc := services.ClusterStatistic{
		StartDate: ctx.Query("start_date"),
		EndDate:   ctx.Query("end_date"),
		StartSize: ctx.Query("start_size"),
		EndSize:   ctx.Query("end_size"),
	}

	data, code, err := svc.ClusterStatisticDetail(ctx, "cluster", queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// ClusterStatisticGroupByBu		查询bu维度集群的大表统计信息
// @Router							/cluster/statistic/bu [get]
// @Description						查询集群维度集群的大表统计信息
// @Tags							统计信息
// @Param   						start_date		query		string     	false  	"统计开始日期"
// @Param   						end_date		query		string     	false  	"统计结束日期"
// @Param   						start_size		query		string     	false  	"统计大于等于该值的表"
// @Param   						end_size		query		string     	false  	"统计小于等于该值的表"
// @Param   						bu				query		string     	false  	"BU"
// @Success							200				{object}	common.Response{data=services.ClusterStatistic}
// @Failure							500				{object}	common.Response
func (c *ClusterStatisticController) ClusterStatisticGroupByBu(ctx *gin.Context) {
	queryMap := make(map[string]string, 10)
	queryMap["bu"] = ctx.Query("bu")

	svc := services.ClusterStatistic{
		StartDate: ctx.Query("start_date"),
		EndDate:   ctx.Query("end_date"),
		StartSize: ctx.Query("start_size"),
		EndSize:   ctx.Query("end_size"),
	}
	data, code, err := svc.ClusterStatisticDetail(ctx, "bu", queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}

// ClusterStatisticGroupByDate		查询bu维度集群的大表统计信息
// @Router							/cluster/statistic/date [get]
// @Description						查询集群维度集群的大表统计信息
// @Tags							统计信息
// @Param   						start_date		query		string     	false  	"统计开始日期"
// @Param   						end_date		query		string     	false  	"统计结束日期"
// @Param   						start_size		query		string     	false  	"统计大于等于该值的表"
// @Param   						end_size		query		string     	false  	"统计小于等于该值的表"
// @Success							200				{object}	common.Response{data=services.ClusterStatistic}
// @Failure							500				{object}	common.Response
func (c *ClusterStatisticController) ClusterStatisticGroupByDate(ctx *gin.Context) {
	queryMap := make(map[string]string, 10)
	svc := services.ClusterStatistic{
		StartDate: ctx.Query("start_date"),
		EndDate:   ctx.Query("end_date"),
		StartSize: ctx.Query("start_size"),
		EndSize:   ctx.Query("end_size"),
	}
	data, code, err := svc.ClusterStatisticDetail(ctx, "date", queryMap)
	if err != nil {
		ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Error: err.Error()})
		return
	}
	ctx.JSON(common.ServiceCode2HttpCode(code), common.Response{ServiceCode: code, Data: data})
	return
}
