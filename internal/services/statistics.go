package services

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"sort"
	"strconv"
	"strings"
	"time"
)

// TaskStatisticService 任务统计信息
type TaskStatisticService struct {
	StartDate string `json:"start_date"`
	EndDate   string `json:"end_date"`
	Success   int    `json:"success"`   // 执行成功个数
	Fail      int    `json:"fail"`      // 失败任务个数
	Executing int    `json:"executing"` // 执行中任务个数
	Upcoming  int    `json:"upcoming"`  // 待执行任务个数
}

// TaskPlanService 任务统计信息
type TaskPlanService struct {
	StartDate string  `json:"start_date"`
	EndDate   string  `json:"end_date"`
	Count     int     `json:"count"`
	Tasks     []*Task `json:"tasks"`
}

type Task struct {
	ID                 uint                  `json:"id"` // ID
	Name               string                `json:"name"`
	TaskStartTime      string                `json:"task_start_time"`      // 开始执行时间
	TaskEndTime        string                `json:"task_end_time"`        // 执行结束时间
	TaskStatus         common.TaskStatusType `json:"task_status"`          // 任务状态
	TaskResultQuantity int                   `json:"task_result_quantity"` // 治理数据量
	TaskResultSize     int                   `json:"task_result_size"`     // 治理数据容量
}

// TaskStatisticDetail 任务统计信息
type TaskStatisticDetail struct {
	StartDate string            `json:"start_date"`
	EndDate   string            `json:"end_date"`
	Total     int               `json:"total"`
	Data      []StatisticDetail `json:"data"`
}

type StatisticDetail struct {
	Bu            string `json:"bu"`
	ClusterName   string `json:"cluster_name"`
	Database      string `json:"database"`
	Table         string `json:"table"`
	Govern        string `json:"govern"`
	TotalQuantity int64  `json:"total_quantity"`
	TotalSize     int64  `json:"total_size"`
	Count         int64  `json:"count"`
}

// PolicyStatistic 策略统计信息
type PolicyStatistic struct {
	Total int                     `json:"total"`
	Data  []PolicyStatisticDetail `json:"data"`
}

// PolicyStatisticDetail 策略统计信息
type PolicyStatisticDetail struct {
	Bu          string `json:"bu"`
	ClusterName string `json:"cluster_name"`
	Database    string `json:"database"`
	Table       string `json:"table"`
	Govern      string `json:"govern"`
	Period      string `json:"period"`
	Condition   string `json:"condition"`
	Count       int64  `json:"count"` // 策略个数
}

// ClusterStatistic 集群的大表统计信息
type ClusterStatistic struct {
	StartDate string                   `json:"start_date"`
	EndDate   string                   `json:"end_date"`
	StartSize string                   `json:"start_size"`
	EndSize   string                   `json:"end_size"`
	Total     int                      `json:"total"`
	Data      []ClusterStatisticDetail `json:"data"`
}

// ClusterStatisticDetail 大表统计信息
type ClusterStatisticDetail struct {
	Date            string `json:"date"`               // 统计的日期
	Bu              string `json:"bu"`                 // bu
	ClusterID       string `json:"cluster_id"`         // 集群ID
	ClusterName     string `json:"cluster_name"`       // 集群名称
	Database        string `json:"database"`           // 库名
	Table           string `json:"table"`              // 表名
	TableSize       int    `json:"table_size"`         // 表大小(GB)
	BigTableSizeSum int    `json:"big_table_size_sum"` // 本BU大表的总容量(GB)
	TableSizeSum    int    `json:"table_size_sum"`     // 筛选的表的总容量(GB)
	TablesNum       int    `json:"tables_num"`         // 大表个数
	PoliciesNum     int    `json:"policies_num"`       // 策略个数
	Policies        string `json:"policies"`           // 对应的策略
}

func (c *TaskStatisticService) TaskStatisticSummary(ctx *gin.Context) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	startTime, err := time.ParseInLocation(time.DateOnly, c.StartDate, time.Now().Location())
	if err != nil {
		log.Warnf("parse StartDate(%s) failed, %v)", c.StartDate, err)
		c.StartDate = "1970-01-01"
		startTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.Now().Location())
	}

	endTime, err := time.ParseInLocation(time.DateOnly, c.EndDate, time.Now().Location())
	if err != nil {
		log.Warnf("parse StartDate(%s) failed, %v)", c.StartDate, err)
		endTime = time.Date(time.Now().Year()+1, time.Now().Month(), time.Now().Day(), 23, 59, 59, 0, time.Now().Location())
		c.EndDate = endTime.Format(time.DateOnly)
	} else {
		endTime = time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 23, 59, 59, 0, time.Now().Location())
	}

	// 成功的个数：统计结束时间在统计区间的的成功的任务个数
	var count int64
	err = db.Model(&models.Task{}).
		Where("task_end_time >= ? AND task_end_time <= ? AND task_status =?", startTime, endTime, common.TaskStatusSuccess).
		Count(&count).Error
	if err != nil {
		err = fmt.Errorf("query models.Task from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}
	c.Success = int(count)

	// 失败的任务个数：统计结束时间在统计区间的的失败或超时任务个数
	err = db.Model(&models.Task{}).
		Where("task_end_time >= ? AND task_end_time <= ? AND task_status IN (?)",
			startTime, endTime, []common.TaskStatusType{common.TaskStatusExecFailed, common.TaskStatusTimeout}).
		Count(&count).Error
	if err != nil {
		err = fmt.Errorf("query models.Task from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}
	c.Fail = int(count)

	// 执行中的任务个数：统计开始时间在统计区间内处于执行中状态的任务个数
	err = db.Model(&models.Task{}).
		Where("task_start_time >= ? AND task_start_time<= ? AND task_status IN (?)",
			startTime, endTime, []common.TaskStatusType{common.TaskStatusExecuting}).
		Count(&count).Error
	if err != nil {
		err = fmt.Errorf("query models.Task from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}
	c.Executing = int(count)

	// 等待执行的任务个数：统计开始时间在统计区间内处于等待执行和执行前检查失败状态的任务个数
	err = db.Model(&models.Task{}).
		Where("execute_date >= ? AND execute_date<= ? AND task_status IN (?)",
			c.StartDate, c.EndDate, common.TaskStatusCanUpdate).
		Count(&count).Error
	if err != nil {
		err = fmt.Errorf("query models.Task from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}
	c.Upcoming = int(count)

	return c, common.CodeOK, nil
}

func (c *TaskPlanService) TaskExecPlan(ctx *gin.Context) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	startTime, err := time.ParseInLocation(time.DateOnly, c.StartDate, time.Now().Location())
	if err != nil {
		log.Error("parse StartDate(%s) failed, %v)", c.StartDate, err)
		c.StartDate = "1970-01-01"
		startTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.Now().Location())
	}

	endTime, err := time.ParseInLocation(time.DateOnly, c.EndDate, time.Now().Location())
	if err != nil {
		log.Error("parse StartDate(%s) failed, %v)", c.StartDate, err)
		endTime = time.Date(time.Now().Year()+1, time.Now().Month(), time.Now().Day(), 23, 59, 59, 0, time.Now().Location())
		c.EndDate = endTime.Format(time.DateOnly)
	} else {
		endTime = time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 23, 59, 59, 0, time.Now().Location())
	}

	// 已经执行完成的
	var tasks1 []models.Task
	err = db.Model(&models.Task{}).
		Select("id, name, execute_date, task_start_time, task_end_time, task_status, task_result_quantity, task_result_size").
		Where("task_start_time >= ? AND task_end_time <= ? AND task_status IN (?)",
			startTime, endTime, common.TaskStatusHasFinished).
		Find(&tasks1).Error
	if err != nil {
		err = fmt.Errorf("query models.Task from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	// 正在执行的
	var tasks2 []models.Task
	err = db.Model(&models.Task{}).
		Select("id, name, execute_date, task_start_time, task_end_time, task_status, task_result_quantity, task_result_size, execute_window").
		Where("execute_date >= ? AND execute_date <= ? AND task_status IN (?)", c.StartDate, c.EndDate, common.TaskStatusExecuting).
		Find(&tasks2).Error
	if err != nil {
		err = fmt.Errorf("query models.Task from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	// 还未开始执行的
	var tasks3 []models.Task
	err = db.Model(&models.Task{}).
		Select("id, name, execute_date, task_start_time, task_end_time, task_status, task_result_quantity, task_result_size, execute_window").
		Where("execute_date >= ? AND execute_date <= ? AND task_status IN (?)",
			c.StartDate, c.EndDate, []common.TaskStatusType{common.TaskStatusScheduled, common.TaskStatusSupplementFailed, common.TaskStatusWaiting, common.TaskStatusExecCheckFailed}).
		Find(&tasks3).Error
	if err != nil {
		err = fmt.Errorf("query models.Task from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	tasks1 = append(tasks1, tasks2...)
	tasks1 = append(tasks1, tasks3...)

	for _, task := range tasks1 {
		start, end := "", ""

		switch task.TaskStatus {
		case common.TaskStatusScheduled, common.TaskStatusSupplementFailed, common.TaskStatusWaiting, common.TaskStatusExecCheckFailed:
			execWin := make([]string, 0, 2)
			_ = json.Unmarshal(task.ExecuteWindow, &execWin)
			start = task.ExecuteDate + " " + execWin[0]
			end = task.ExecuteDate + " " + execWin[1]
		case common.TaskStatusExecuting:
			execWin := make([]string, 0, 2)
			_ = json.Unmarshal(task.ExecuteWindow, &execWin)
			start = task.TaskStartTime.Format(time.DateTime)
			end = task.ExecuteDate + " " + execWin[1]
		default:
			if task.TaskStartTime.IsZero() || task.TaskEndTime.IsZero() {
				continue
			}
			start = task.TaskStartTime.Format(time.DateTime)
			end = task.TaskEndTime.Format(time.DateTime)
		}

		t := &Task{
			ID:                 task.ID,
			Name:               task.Name,
			TaskStartTime:      start,
			TaskEndTime:        end,
			TaskStatus:         task.TaskStatus,
			TaskResultQuantity: task.TaskResultQuantity,
			TaskResultSize:     task.TaskResultSize,
		}

		if t.TaskEndTime <= t.TaskStartTime {
			if tmp, err := time.ParseInLocation(time.DateTime, t.TaskEndTime, time.Now().Location()); err == nil {
				t.TaskEndTime = tmp.Add(time.Hour * 24).Format(time.DateTime)
			}
		}

		c.Tasks = append(c.Tasks, t)
	}
	c.Count = len(c.Tasks)
	return c, common.CodeOK, nil
}

func (c *TaskStatisticDetail) TaskStatisticDetail(ctx *gin.Context, groupBy string, queryMap map[string]string) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	startTime, err := time.ParseInLocation(time.DateOnly, c.StartDate, time.Now().Location())
	if err != nil {
		log.Error("parse StartDate(%s) failed, %v)", c.StartDate, err)
		c.StartDate = "1970-01-01"
		startTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.Now().Location())
	}

	endTime, err := time.ParseInLocation(time.DateOnly, c.EndDate, time.Now().Location())
	if err != nil {
		log.Error("parse StartDate(%s) failed, %v)", c.StartDate, err)
		endTime = time.Date(time.Now().Year()+1, time.Now().Month(), time.Now().Day(), 23, 59, 59, 0, time.Now().Location())
		c.EndDate = endTime.Format(time.DateOnly)
	} else {
		endTime = time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 23, 59, 59, 0, time.Now().Location())
	}

	sel, group, genKey := "", "", func(d *StatisticDetail) string { return "" }
	switch groupBy {
	case "bu":
		sel = "src_bu AS bu, govern, SUM(task_result_quantity) AS total_quantity, SUM(task_result_size) AS total_size, count(id) AS count"
		group = "src_bu, govern"
		genKey = func(d *StatisticDetail) string {
			return d.Bu
		}
	case "cluster":
		sel = "src_bu AS bu, src_cluster_name AS cluster_name, govern, SUM(task_result_quantity) AS total_quantity, SUM(task_result_size) AS total_size, count(id) AS count"
		group = "src_bu, src_cluster_name, govern"
		genKey = func(d *StatisticDetail) string {
			return strings.Join([]string{d.Bu, d.ClusterName}, "::::")
		}
	case "database":
		sel = "src_bu AS bu, src_cluster_name AS cluster_name, src_database_name AS `database`, govern, SUM(task_result_quantity) AS total_quantity, SUM(task_result_size) AS total_size, count(id) AS count"
		group = "src_bu, src_cluster_name, src_database_name, govern"
		genKey = func(d *StatisticDetail) string {
			return strings.Join([]string{d.Bu, d.ClusterName, d.Database}, "::::")
		}
	case "table":
		sel = "src_bu AS bu, src_cluster_name AS cluster_name, src_database_name AS `database`, src_tables_name AS `table`, govern, SUM(task_result_quantity) AS total_quantity, SUM(task_result_size) AS total_size, count(id) AS count"
		group = "src_bu, src_cluster_name, src_database_name, src_tables_name, govern"
		genKey = func(d *StatisticDetail) string {
			return strings.Join([]string{d.Bu, d.ClusterName, d.Database, d.Table}, "::::")
		}
	default:
		return c, common.CodeOK, fmt.Errorf("unsupported query method %s", groupBy)
	}

	var res []StatisticDetail
	err = db.Model(&models.Task{}).
		Select(sel).
		//Where("task_start_time >= ? AND task_end_time <= ? AND task_status IN (?)", startTime, endTime, common.TaskStatusHasFinished).
		Where("task_start_time >= ? AND task_start_time <= ? AND task_status IN (?)", startTime, endTime, common.TaskStatusHasFinished).
		Scopes(common.FilterFuzzyStringMap(queryMap)).
		Group(group).
		Scan(&res).Error
	if err != nil {
		err = fmt.Errorf("query models.Task from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	tableGovernMap := make(map[string]*StatisticDetail, len(res))
	for i := range res {
		if b, table, err := common.CheckSameShardingTables(res[i].Table); err == nil {
			if b {
				res[i].Table = table + fmt.Sprintf("(%d个分表)", len(strings.Split(res[i].Table, ",")))
			} else {
				res[i].Table = table
			}
		}

		key := genKey(&res[i])
		if val, ok := tableGovernMap[key]; ok {
			val.TotalQuantity += res[i].TotalQuantity
			val.TotalSize += res[i].TotalSize
			val.Count += res[i].Count
			val.Govern = utils.TrimmingStringList(val.Govern+","+res[i].Govern, ",")
		} else {
			tableGovernMap[key] = &res[i]
		}
	}

	c.Total = len(tableGovernMap)
	c.Data = make([]StatisticDetail, 0, len(tableGovernMap))
	for _, v := range tableGovernMap {
		c.Data = append(c.Data, *v)
	}

	sort.Slice(c.Data, func(i, j int) bool {
		if c.Data[i].Bu < c.Data[j].Bu {
			return true
		} else if c.Data[i].Bu > c.Data[j].Bu {
			return false
		}

		if c.Data[i].ClusterName < c.Data[j].ClusterName {
			return true
		} else if c.Data[i].ClusterName > c.Data[j].ClusterName {
			return false
		}

		if c.Data[i].Database < c.Data[j].Database {
			return true
		} else if c.Data[i].Database > c.Data[j].Database {
			return false
		}

		if c.Data[i].Table < c.Data[j].Table {
			return true
		} else if c.Data[i].Table > c.Data[j].Table {
			return false
		}

		if c.Data[i].TotalQuantity < c.Data[j].TotalQuantity {
			return true
		} else if c.Data[i].TotalQuantity > c.Data[j].TotalQuantity {
			return false
		}

		if c.Data[i].TotalSize < c.Data[j].TotalSize {
			return true
		} else if c.Data[i].TotalSize > c.Data[j].TotalSize {
			return false
		}

		if c.Data[i].Count < c.Data[j].Count {
			return true
		} else if c.Data[i].Count > c.Data[j].Count {
			return false
		}
		return false
	})

	return c, common.CodeOK, nil
}

func (c *PolicyStatistic) PolicyStatisticDetail(ctx *gin.Context, groupBy string, queryMap map[string]string) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	enable, enableOk := ctx.GetQuery("enable")

	sel, group := "", ""
	switch groupBy {
	case "bu":
		sel = "source.bu AS bu, policy.govern AS govern, count(policy.id) AS count"
		group = "source.bu,  policy.govern"
	case "cluster":
		sel = "source.bu AS bu, source.cluster_name AS cluster_name, policy.govern AS govern, count(policy.id) AS count"
		group = "source.bu, source.cluster_name, policy.govern"
	case "database":
		sel = "source.bu AS bu, source.cluster_name AS cluster_name, source.database_name AS `database`, policy.govern AS govern, count(policy.id) AS count"
		group = "source.bu, source.cluster_name, source.database_name, policy.govern"
	case "table":
		sel = "source.bu AS bu, source.cluster_name AS cluster_name, source.database_name AS `database`, source.tables_name AS `table`, policy.govern AS govern, policy.period AS period, policy.`condition` AS `condition`, count(policy.id) AS count"
		group = "source.bu, source.cluster_name, source.database_name, source.tables_name, policy.govern, policy.period, policy.`condition`"
	default:
		return c, common.CodeOK, fmt.Errorf("unsupported query method %s", groupBy)
	}

	var res []PolicyStatisticDetail
	err := db.Model(&models.Policy{}).
		Select(sel).
		Joins("JOIN source ON policy.src_id = source.id").
		Scopes(
			common.FilterCustomBool("enable", enable, enableOk),
			common.FilterFuzzyStringMap(queryMap),
		).
		Group(group).
		Scan(&res).Error
	if err != nil {
		err = fmt.Errorf("query models.Policy from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	for i, v := range res {
		if b, table, err := common.CheckSameShardingTables(v.Table); err == nil {
			if b {
				res[i].Table = table + fmt.Sprintf("(%d个分表)", len(strings.Split(v.Table, ",")))
			} else {
				res[i].Table = table
			}
		}
	}
	c.Total = len(res)
	c.Data = res

	sort.Slice(c.Data, func(i, j int) bool {
		if c.Data[i].Bu < c.Data[j].Bu {
			return true
		} else if c.Data[i].Bu > c.Data[j].Bu {
			return false
		}

		if c.Data[i].ClusterName < c.Data[j].ClusterName {
			return true
		} else if c.Data[i].ClusterName > c.Data[j].ClusterName {
			return false
		}

		if c.Data[i].Database < c.Data[j].Database {
			return true
		} else if c.Data[i].Database > c.Data[j].Database {
			return false
		}

		if c.Data[i].Table < c.Data[j].Table {
			return true
		} else if c.Data[i].Table > c.Data[j].Table {
			return false
		}

		if c.Data[i].Count < c.Data[j].Count {
			return true
		} else if c.Data[i].Count > c.Data[j].Count {
			return false
		}

		if c.Data[i].Condition < c.Data[j].Condition {
			return true
		} else if c.Data[i].Condition > c.Data[j].Condition {
			return false
		}

		return true
	})

	return c, common.CodeOK, nil
}

func (c *ClusterStatistic) ClusterStatisticDetail(ctx *gin.Context, groupBy string, queryMap map[string]string) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	_, err := time.ParseInLocation(time.DateOnly, c.StartDate, time.Now().Location())
	if err != nil {
		log.Error("parse StartDate(%s) failed, %v)", c.StartDate, err)
		c.StartDate = "1970-01-01"
	}

	endTime, err := time.ParseInLocation(time.DateOnly, c.EndDate, time.Now().Location())
	if err != nil {
		log.Error("parse StartDate(%s) failed, %v)", c.StartDate, err)
		endTime = time.Date(time.Now().Year()+1, time.Now().Month(), time.Now().Day(), 23, 59, 59, 0, time.Now().Location())
		c.EndDate = endTime.Format(time.DateOnly)
	}

	if num, err := strconv.Atoi(c.StartSize); err != nil {
		c.StartSize = ""
	} else {
		c.StartSize = strconv.Itoa(num)
	}

	if num, err := strconv.Atoi(c.EndSize); err != nil {
		c.EndSize = ""
	} else {
		c.EndSize = strconv.Itoa(num)
	}

	var tableSizeByBu []models.ClusterStatistics
	err = db.Model(models.ClusterStatistics{}).
		Select("date, bu, SUM(table_size) as table_size").
		Where("date >= ? AND date <= ?", c.StartDate, c.EndDate).
		Group("date, bu").
		Find(&tableSizeByBu).
		Error
	if err != nil {
		err = fmt.Errorf("query models.ClusterStatistics from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	tableSizeByBuMap := make(map[string]int, len(tableSizeByBu))
	for _, v := range tableSizeByBu {
		tableSizeByBuMap[fmt.Sprintf("%s_%s", v.Date, v.Bu)] = v.TableSize
	}

	var res []models.ClusterStatistics
	err = db.Model(models.ClusterStatistics{}).
		Where("date >= ? AND date <= ?", c.StartDate, c.EndDate).
		Scopes(
			common.FilterFuzzyStringMap(queryMap),
			common.FilterDataRange("table_size", c.StartSize, c.EndSize),
		).
		Scan(&res).Error
	if err != nil {
		err = fmt.Errorf("query models.ClusterStatistics from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	clusterMap := make(map[string]*ClusterStatisticDetail, len(res))

	genKey := func(d *models.ClusterStatistics) string { return "" }
	switch groupBy {
	case "date":
		genKey = func(d *models.ClusterStatistics) string {
			return d.Date
		}
	case "bu":
		genKey = func(d *models.ClusterStatistics) string {
			return strings.Join([]string{d.Date, d.Bu}, "::::")
		}
	case "cluster":
		genKey = func(d *models.ClusterStatistics) string {
			return strings.Join([]string{d.Date, d.Bu, d.ClusterName}, "::::")
		}
	case "database":
		genKey = func(d *models.ClusterStatistics) string {
			return strings.Join([]string{d.Date, d.Bu, d.ClusterName, d.Database}, "::::")
		}
	case "table":
		genKey = func(d *models.ClusterStatistics) string {
			return strings.Join([]string{d.Date, d.Bu, d.ClusterName, d.Database, d.Table}, "::::")
		}
	default:
		return c, common.CodeOK, fmt.Errorf("unsupported query method %s", groupBy)
	}

	for _, r := range res {
		key := genKey(&r)
		if val, ok := clusterMap[key]; ok {
			val.TablesNum++
			val.PoliciesNum += utils.Ternary[int](utils.CountSubString(r.Policies, ",") > 0, 1, 0)
			val.TableSizeSum += r.TableSize
		} else {
			cs := &ClusterStatisticDetail{
				Date:            r.Date,
				Bu:              r.Bu,
				ClusterID:       r.ClusterID,
				ClusterName:     r.ClusterName,
				Database:        r.Database,
				Table:           r.Table,
				TableSize:       r.TableSize,
				TableSizeSum:    r.TableSize,
				BigTableSizeSum: tableSizeByBuMap[fmt.Sprintf("%s_%s", r.Date, r.Bu)],
				Policies:        r.Policies,
				TablesNum:       1,
				PoliciesNum:     utils.Ternary[int](utils.CountSubString(r.Policies, ",") > 0, 1, 0),
			}

			switch groupBy {
			case "date":
				cs.Bu = ""
				fallthrough
			case "bu":
				cs.ClusterName = ""
				cs.ClusterID = ""
				fallthrough
			case "cluster":
				cs.Database = ""
				fallthrough
			case "database":
				cs.Table = ""
				cs.TableSize = 0
				cs.Policies = ""
			}

			clusterMap[key] = cs
		}
	}

	c.Total = len(clusterMap)
	c.Data = make([]ClusterStatisticDetail, 0, len(clusterMap))
	for _, v := range clusterMap {
		c.Data = append(c.Data, *v)
	}

	sort.Slice(c.Data, func(i, j int) bool {
		if c.Data[i].Date < c.Data[j].Date {
			return true
		} else if c.Data[i].Date > c.Data[j].Date {
			return false
		}

		if c.Data[i].Bu < c.Data[j].Bu {
			return true
		} else if c.Data[i].Bu > c.Data[j].Bu {
			return false
		}

		if c.Data[i].ClusterName < c.Data[j].ClusterName {
			return true
		} else if c.Data[i].ClusterName > c.Data[j].ClusterName {
			return false
		}

		if c.Data[i].Database < c.Data[j].Database {
			return true
		} else if c.Data[i].Database > c.Data[j].Database {
			return false
		}

		if c.Data[i].Table < c.Data[j].Table {
			return true
		} else if c.Data[i].Table > c.Data[j].Table {
			return false
		}

		if c.Data[i].TablesNum < c.Data[j].TablesNum {
			return true
		} else if c.Data[i].TablesNum > c.Data[j].TablesNum {
			return false
		}

		return true
	})

	return c, common.CodeOK, nil
}
