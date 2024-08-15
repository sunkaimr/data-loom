package services

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/configs"
	. "github.com/sunkaimr/data-loom/internal/job/status"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	. "github.com/sunkaimr/data-loom/internal/pkg/queue"
	"github.com/sunkaimr/data-loom/internal/workflow"
	"github.com/sunkaimr/data-loom/internal/workflow/types"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"gorm.io/gorm"
	"strings"
	"time"
)

type TaskService struct {
	// 归档任务元数据信息
	Model
	Name               string                `json:"name"`                 // 任务名称
	Description        string                `json:"description"`          // 说明
	Enable             bool                  `json:"enable"`               // 是否生效
	PolicyID           uint                  `json:"policy_id"`            // 策略ID
	ExecuteWindow      []string              `json:"execute_window"`       // 执行窗口
	ExecuteDate        string                `json:"execute_date"`         // 预计执行日期 "2024-01-02"
	Pause              bool                  `json:"pause"`                // 执行窗口外是否需要暂停执行
	RebuildFlag        bool                  `json:"rebuild_flag"`         // 执行窗口外是否重建表(仅在治理方式是删除时有效)。true:在执行窗口外仍然走重建流程; false:执行窗口外跳过重建流程
	TaskStatus         common.TaskStatusType `json:"task_status"`          // 任务状态
	TaskReason         string                `json:"task_reason"`          // 任务失败原因
	TaskDetail         string                `json:"task_detail"`          // 任务失败详情
	TaskResultQuantity int                   `json:"task_result_quantity"` // 治理数据量
	TaskResultSize     int                   `json:"task_result_size"`     // 治理数据大小
	TaskStartTime      string                `json:"task_start_time"`      // 开始执行时间
	TaskEndTime        string                `json:"task_end_time"`        // 执行结束时间
	TaskDuration       int                   `json:"task_duration"`        // 执行时长(秒)
	WorkFlow           string                `json:"workflow"`             // 工作流
	WorkFlowURL        string                `json:"workflow_url"`         // 工作流地址

	// 源端信息
	SrcID           uint   `json:"src_id"`            // 任务ID
	SrcName         string `json:"src_name"`          // 源端名称
	SrcBu           string `json:"src_bu"`            // 资产BU
	SrcClusterName  string `json:"src_cluster_name"`  // 集群名称
	SrcClusterID    string `json:"src_cluster_id"`    // 集群ID
	SrcAddr         string `json:"src_addr"`          // 源端地址
	SrcDatabaseName string `json:"src_database_name"` // 源库名
	SrcTablesName   string `json:"src_tables_name"`   // 源表名
	SrcColumns      string `json:"src_columns"`       // 源列名

	// 目标端信息
	DestID           uint               `json:"dest_id"`            // 目标端ID
	DestName         string             `json:"dest_name"`          // 目标端名称
	DestStorage      common.StorageType `json:"dest_storage"`       // 归档介质
	DestConnectionID uint               `json:"dest_connection_id"` // 归档库连接信息
	DestDatabaseName string             `json:"dest_database_name"` // 归档库名
	DestTableName    string             `json:"dest_table_name"`    // 归档表名
	DestCompress     bool               `json:"dest_compress"`      // 是否压缩存储

	// 数据治理方式
	Govern        common.GovernType        `json:"govern" `         // 数据治理方式 清空数据:truncate, 不备份清理:delete, 备份后清理:backup-delete, 归档:archive
	Condition     string                   `json:"condition"`       // 数据治理条件
	RetainSrcData bool                     `json:"retain_src_data"` //归档时否保留源表数据
	CleaningSpeed common.CleaningSpeedType `json:"cleaning_speed"`  // 清理速度 稳定优先:steady, 速度适中:balanced, 速度优先:swift

	// 结果通知
	Relevant     []string                `json:"relevant"`      // 关注人
	NotifyPolicy common.NotifyPolicyType `json:"notify_policy"` // 通知策略 不通知:silence, 成功时通知:success, 失败时通知:failed, 成功或失败都通知:always
}

func (c *TaskService) CheckUpdateParameters(ctx *gin.Context) (bool, common.ServiceCode, error) {
	var err error
	log, db := common.ExtractContext(ctx)

	if common.InvalidUintID(c.ID) {
		return false, common.CodeInvalidID, fmt.Errorf("invalid Task.id(%d)", c.ID)
	}

	if len(c.Name) == 0 || len(c.Name) == 1024 {
		return false, common.CodeTaskNameLenErr, fmt.Errorf("validate name(%s) not pass", c.Name)
	}

	task := &models.Task{}
	err = db.Model(task).First(task, "id = ?", c.ID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.Task(id=%d) not exist", task.ID)
			log.Error(err)
			return false, common.CodeTaskNotExist, err
		}
		err = fmt.Errorf("query models.Task(id=%d) from db failed, %s", task.ID, err)
		log.Error(err)
		return false, common.CodeServerErr, err
	}

	if !utils.ElementExist(task.TaskStatus, common.TaskStatusCanUpdate) {
		err = fmt.Errorf("the task(%v) in the current status(%s) cannot be modified", task.ID, task.TaskStatus)
		log.Error(err)
		return false, common.CodeTaskStatusImmutable, err
	}

	if c.NotifyPolicy != "" && !common.CheckNotifyPolicyType(c.NotifyPolicy) {
		return false, common.CodeTaskNotifyPolicyErr, fmt.Errorf("validate notify_policy(%s) not pass", c.NotifyPolicy)
	}

	executeDate, err := time.ParseInLocation(time.DateOnly, c.ExecuteDate, time.Now().Location())
	if err != nil {
		return false, common.CodeTaskExecDateErr, fmt.Errorf("parse execute date(%s) faield, it should like '%s', %s", c.ExecuteDate, time.DateOnly, err)
	}
	c.ExecuteDate = executeDate.Format(time.DateOnly)

	_, execWin, err := checkExecWindow(c.ExecuteWindow)
	if err != nil {
		return false, common.CodePolicyExecuteWindowErr, err
	}
	c.ExecuteWindow = execWin
	task.TaskStatus = common.TaskStatusScheduled

	return true, common.CodeOK, nil
}

func (c *TaskService) UpdateTask(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	task := &models.Task{}
	err := db.Model(&task).First(task, "id = ?", c.ID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.Task(id=%d) not exist", task.ID)
			log.Error(err)
			return common.CodeDestNotExist, err
		}
		err = fmt.Errorf("query models.Task(id=%d) from db failed, %s", task.ID, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	newTask := *task
	newTask.Name = c.Name
	newTask.Description = c.Description
	newTask.RebuildFlag = c.RebuildFlag
	newTask.ExecuteDate = c.ExecuteDate
	newTask.ExecuteWindow, _ = json.Marshal(c.ExecuteWindow)
	newTask.Editor = u.UserName
	newTask.Enable = c.Enable
	newTask.NotifyPolicy = utils.Ternary[common.NotifyPolicyType](c.NotifyPolicy == "", task.NotifyPolicy, c.NotifyPolicy)
	if len(c.Relevant) != 0 {
		newTask.Relevant, _ = json.Marshal(c.Relevant)
	}

	err = db.Transaction(func(db *gorm.DB) error {
		err = db.Save(&newTask).Error
		if err != nil {
			log.Errorf("update models.Task(%+v) from db failed, %s", task, err)
			return err
		}

		_, err = new(TaskRevisionService).CreateTaskRevision(ctx, task, &newTask)
		if err != nil {
			log.Errorf("save models.TaskRevision to db failed, %s", err)
			return err
		}
		return nil
	})
	if err != nil {
		return common.CodeServerErr, err
	}

	if task.ExecuteDate != newTask.ExecuteDate || string(task.ExecuteWindow) != string(newTask.ExecuteWindow) {
		CreateTaskChangeLog(common.NewContext().WithDB(db).WithLog(log), task, u.RealName, fmt.Sprintf(common.TaskChangeLogUpdate, newTask.ExecuteDate, string(newTask.ExecuteWindow)))
	}

	if newTask.Enable {
		// 提前一天检查任务是否满足执行条件，以便于人可以提前介入修复异常
		// 如果距离执行时间不足1天直接进行调度前检查，否则修改任务状态为已排期走正常的检查流程

		// 任务不是一天后执行先不用检查
		if common.JudgeTaskCouldCheckBeforeExec(newTask.ExecuteDate) {
			// 	因为任务将在一天后执行需通知TaskHandle立即进行调度前检查
			handle := &TaskQueueHandle{ID: newTask.ID, PolicyID: newTask.PolicyID, HandleID: ctx.GetHeader(common.RequestID)}
			log.With(WithExtra[*TaskQueueHandle](handle, ProcQueuing)...).Infof("task %s push queue...", newTask.Name)
			ok := TaskQueue.Push(*handle)
			if ok {
				log.With(WithExtra[*TaskQueueHandle](handle, ProcNotQueue)...).Infof("task %s push queue sucess", newTask.Name)
			} else {
				log.With(WithExtra[*TaskQueueHandle](handle, ProcQueued)...).Infof("task %s not push queue", newTask.Name)
			}
		}
	}

	c.ModelToService(&newTask)

	return common.CodeOK, nil
}

func (c *TaskService) QueryTask(ctx *gin.Context, queryMap map[string]string) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	enable, enableOk := ctx.GetQuery("enable")
	pause, pauseOk := ctx.GetQuery("pause")
	rebuildFlag, rebuildFlagOk := ctx.GetQuery("rebuild_flag")
	retainSrc, retainSrcOk := ctx.GetQuery("retain_src_data")
	destCompress, destCompressOk := ctx.GetQuery("dest_compress")
	taskStatus := ctx.QueryArray("task_status")
	taskStatus = utils.RemoveSubSlices(taskStatus, []string{""})

	res, err := common.NewPageList[[]models.Task](db).
		QueryPaging(ctx).
		Order("id desc").
		Query(
			common.FilterFuzzyStringMap(queryMap),
			common.FilterID(c.ID),
			common.FilterMultiCondition("task_status", taskStatus),
			common.FilterCustomUintID("policy_id", c.PolicyID),
			common.FilterCustomUintID("src_id", c.SrcID),
			common.FilterCustomUintID("dest_id", c.DestID),
			common.FilterCustomUintID("dest_connection_id", c.DestConnectionID),
			common.FilterCustomBool("enable", enable, enableOk),
			common.FilterCustomBool("pause", pause, pauseOk),
			common.FilterCustomBool("rebuild_flag", rebuildFlag, rebuildFlagOk),
			common.FilterCustomBool("retain_src_data", retainSrc, retainSrcOk),
			common.FilterCustomBool("dest_compress", destCompress, destCompressOk),
		)
	if err != nil {
		err = fmt.Errorf("query models.Policy from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	ret := common.NewPageList[[]TaskService](db)
	ret.Page = res.Page
	ret.PageSize = res.PageSize
	ret.Total = res.Total
	for i := range res.Items {
		t := &TaskService{}
		t.ModelToService(&res.Items[i])
		t.WorkFlowURL, _ = workflow.NewDriver(configs.C.WorkFlow.Driver).WorkFlowAddr(nil, t.WorkFlow)
		ret.Items = append(ret.Items, *t)
	}

	return ret, common.CodeOK, nil
}

func (c *TaskService) DeleteTask(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)
	var err error

	task := &models.Task{}

	// 若ID有效则以ID为准, 否则以name为准
	if !common.InvalidUintID(c.ID) {
		err = db.Model(task).First(task, "id = ?", c.ID).Error
	} else {
		err = db.Model(task).First(task, "name = ?", c.Name).Error
	}
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Infof("query models.Task(%v|%v) not exist", c.ID, c.Name)
			return common.CodeTaskNotExist, err
		}
		err = fmt.Errorf("query models.Task(%v|%v) from db failed, %s", c.ID, c.Name, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	// 执行中的任务不能删除
	if task.TaskStatus == common.TaskStatusExecuting {
		err = fmt.Errorf("the task(%v) in the current state(%s) cannot be deleted", c.ID, task.TaskStatus)
		log.Error(err)
		return common.CodeTaskStatusNoDelete, err
	}

	if utils.ElementExist(task.TaskStatus, common.TaskStatusHasFinished) && u.UserName != common.AdminUser {
		err = fmt.Errorf("you(%s) have no permission to delete task has finished, only supports '%s'", u.UserName, common.AdminUser)
		log.Error(err)
		return common.CodeAdminOnly, err
	}

	err = db.Transaction(func(db *gorm.DB) error {
		// 更新任务
		db.Model(&models.Task{}).Where("id =?", task.ID).Update("editor", u.UserName)

		// 删除任务
		err = db.Delete(&models.Task{}, "id =?", task.ID).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("delete models.Task(name=%s) from db failed, %s", task.Name, err)
		}

		// 删除任务修订记录
		err = db.Delete(&models.TaskRevision{}, "task_id =?", task.ID).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("delete models.TaskRevision(task_id=%v) from db failed, %s", task.ID, err)
		}

		// 删除任务状态记录
		err = db.Delete(&models.TaskChangeLog{}, "task_id =?", task.ID).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("delete models.TaskChangeLog(task_id=%v) from db failed, %s", task.ID, err)
		}
		return nil
	})
	if err != nil {
		log.Errorf("delete task(%v) failed, %s", task.ID, err)
	}

	return common.CodeOK, nil
}

func UpdateTaskResult(ctx *gin.Context, c *types.TaskResultService) (*TaskService, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	user := common.ExtractUserInfo(ctx)

	if common.InvalidUintID(c.ID) {
		return nil, common.CodeInvalidID, fmt.Errorf("invalid Task.id(%d)", c.ID)
	}

	task := &models.Task{}
	err := db.Model(&task).First(task, "id = ?", c.ID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.Task(id=%d) not exist", c.ID)
			log.Error(err)
			return nil, common.CodeTaskNotExist, err
		}
		err = fmt.Errorf("query models.Task(id=%d) from db failed, %s", c.ID, err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	if !common.CheckTaskStatusType(c.TaskStatus) {
		err = fmt.Errorf("the task(%v) status(%v) is invalid", c.ID, c.TaskStatus)
		return nil, common.CodeTaskStatusErr, err
	}

	if c.TaskStartTime != "" && c.TaskEndTime != "" {
		s, err1 := time.ParseInLocation(time.DateTime, c.TaskStartTime, time.Now().Location())
		e, err2 := time.ParseInLocation(time.DateTime, c.TaskEndTime, time.Now().Location())
		if err1 == nil || err2 == nil {
			// 传递了任务的起止时间以传递的时间为准，否则以实际时间为准
			task.TaskStartTime = s
			task.TaskEndTime = e
		} else {
			task.TaskEndTime = time.Now()
		}
		task.TaskDuration = int(task.TaskEndTime.Sub(task.TaskStartTime).Seconds())
	} else {
		task.TaskEndTime = time.Now()
		task.TaskDuration = int(task.TaskEndTime.Sub(task.TaskStartTime).Seconds())
	}

	task.TaskStatus = c.TaskStatus
	task.TaskReason = c.TaskReason
	task.TaskDetail = c.TaskDetail
	task.TaskResultQuantity = utils.Ternary[int](c.TaskResultQuantity == common.InvalidInt, task.TaskResultQuantity, c.TaskResultQuantity)

	// 	清理后表大小, 用户提供了则以用户的为准
	if c.TaskResultSize == common.InvalidInt {
		clusterSvc, err := GetClusterServiceByClusterID(ctx, task.SrcClusterID)
		if err != nil {
			log.Error(err)
			return nil, common.CodeServerErr, err
		}
		tableSize, code, err := NewClusterDriver(clusterSvc).GetTablesSize(ctx, task.SrcDatabaseName, strings.Split(task.SrcTablesName, ","))
		if err != nil {
			log.Error(err)
			return nil, code, err
		}
		task.TaskResultSize = task.SrcClusterSumTableSize - tableSize
	} else {
		task.TaskResultSize = c.TaskResultSize
	}

	task.TaskResultSize = utils.Ternary[int](task.TaskResultSize > 0, task.TaskResultSize, 0)

	err = db.Save(&task).Error
	if err != nil {
		log.Errorf("update models.Task(%+v) from db failed, %s", task, err)
		return nil, common.CodeServerErr, err
	}
	CreateTaskChangeLog(common.NewContext().WithDB(db).WithLog(log), task, user.RealName, common.TaskChangeLogWorkFlowFinished)

	status := ProcExecSuccess
	switch task.TaskStatus {
	case common.TaskStatusSuccess:
		status = ProcExecSuccess
	case common.TaskStatusExecFailed:
		status = ProcExecFailed
	}
	log.With(WithExtra[*TaskQueueHandle](&TaskQueueHandle{ID: task.ID, PolicyID: task.PolicyID, HandleID: ctx.GetHeader(common.RequestID)}, status)...).
		Infof("got task(%v) result(%v) form workflow ", task.Name, task.TaskStatus)

	// 任务已经执行完毕通知PolicyHandle立即重新生成下一次的任务
	handle := &PolicyQueueHandle{ID: task.PolicyID, HandleID: ctx.GetHeader(common.RequestID)}
	log.With(WithExtra[*PolicyQueueHandle](handle, ProcQueuing)...).Infof("policy %v push queue...", task.PolicyID)
	ok := PolicyQueue.Push(*handle)
	if ok {
		log.With(WithExtra[*PolicyQueueHandle](handle, ProcQueued)...).Infof("policy %v push queue sucess", task.PolicyID)
	} else {
		log.With(WithExtra[*PolicyQueueHandle](handle, ProcNotQueue)...).Infof("policy %v not push queue", task.PolicyID)
	}

	taskSvc := &TaskService{}
	taskSvc.ModelToService(task)

	return taskSvc, common.CodeOK, nil
}
func (c *TaskService) ServiceToModel() *models.Task {
	m := &models.Task{}
	m.ID = c.ID
	m.Creator = c.Creator
	m.Editor = c.Editor
	m.CreatedAt, _ = time.ParseInLocation(time.DateTime, c.CreatedAt, time.Now().Location())
	m.UpdatedAt, _ = time.ParseInLocation(time.DateTime, c.UpdatedAt, time.Now().Location())
	m.Name = c.Name
	m.Description = c.Description
	m.Enable = c.Enable
	m.PolicyID = c.PolicyID
	m.ExecuteWindow, _ = json.Marshal(c.ExecuteWindow)
	m.Pause = c.Pause
	m.RebuildFlag = c.RebuildFlag
	m.TaskResultQuantity = c.TaskResultQuantity
	m.TaskResultSize = c.TaskResultSize
	m.TaskDuration = c.TaskDuration
	m.WorkFlow = c.WorkFlow
	m.SrcID = c.SrcID
	m.SrcName = c.SrcName
	m.SrcBu = c.SrcBu
	m.SrcClusterName = c.SrcClusterName
	m.SrcClusterID = c.SrcClusterID
	m.SrcDatabaseName = c.SrcDatabaseName
	m.SrcTablesName = c.SrcTablesName
	m.SrcColumns = c.SrcColumns
	m.DestID = c.DestID
	m.DestName = c.DestName
	m.DestStorage = c.DestStorage
	m.DestConnectionID = c.DestConnectionID
	m.DestDatabaseName = c.DestDatabaseName
	m.DestTableName = c.DestTableName
	m.DestCompress = c.DestCompress
	m.Govern = c.Govern
	m.Condition = c.Condition
	m.RetainSrcData = c.RetainSrcData
	m.CleaningSpeed = c.CleaningSpeed
	m.NotifyPolicy = c.NotifyPolicy
	m.ExecuteDate = c.ExecuteDate
	m.TaskStartTime, _ = time.ParseInLocation(time.DateTime, c.TaskStartTime, time.Now().Location())
	m.TaskEndTime, _ = time.ParseInLocation(time.DateTime, c.TaskEndTime, time.Now().Location())
	m.Relevant, _ = json.Marshal(c.Relevant)
	return m
}

func (c *TaskService) ModelToService(m *models.Task) *TaskService {
	c.ID = m.ID
	c.Creator = m.Creator
	c.Editor = m.Editor
	c.CreatedAt = m.CreatedAt.Format(time.DateTime)
	c.UpdatedAt = m.UpdatedAt.Format(time.DateTime)
	c.Name = m.Name
	c.Description = m.Description
	c.ID = m.ID
	c.Name = m.Name
	c.Enable = m.Enable
	c.Pause = m.Pause
	c.RebuildFlag = m.RebuildFlag
	c.PolicyID = m.PolicyID
	c.ExecuteDate = m.ExecuteDate
	c.TaskStatus = m.TaskStatus
	c.TaskReason = m.TaskReason
	c.TaskDetail = m.TaskDetail
	c.TaskResultQuantity = m.TaskResultQuantity
	c.TaskResultSize = m.TaskResultSize
	c.TaskDuration = m.TaskDuration
	c.WorkFlow = m.WorkFlow
	c.SrcID = m.SrcID
	c.SrcName = m.SrcName
	c.SrcBu = m.SrcBu
	c.SrcClusterName = m.SrcClusterName
	c.SrcClusterID = m.SrcClusterID
	c.SrcDatabaseName = m.SrcDatabaseName
	c.SrcTablesName = m.SrcTablesName
	c.SrcColumns = m.SrcColumns
	c.DestID = m.DestID
	c.DestName = m.DestName
	c.DestStorage = m.DestStorage
	c.DestConnectionID = m.DestConnectionID
	c.DestDatabaseName = m.DestDatabaseName
	c.DestTableName = m.DestTableName
	c.DestCompress = m.DestCompress
	c.Govern = m.Govern
	c.Condition = m.Condition
	c.RetainSrcData = m.RetainSrcData
	c.CleaningSpeed = m.CleaningSpeed
	c.NotifyPolicy = m.NotifyPolicy
	_ = json.Unmarshal(m.Relevant, &c.Relevant)
	_ = json.Unmarshal(m.ExecuteWindow, &c.ExecuteWindow)
	c.TaskStartTime = utils.Ternary[string](m.TaskStartTime == time.UnixMilli(0), "", m.TaskStartTime.Format(time.DateTime))
	c.TaskEndTime = utils.Ternary[string](m.TaskEndTime == time.UnixMilli(0), "", m.TaskEndTime.Format(time.DateTime))
	return c
}
