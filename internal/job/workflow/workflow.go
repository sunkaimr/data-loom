package workflow

import (
	"encoding/json"
	"fmt"
	"github.com/golang-jwt/jwt/v5"
	"github.com/sunkaimr/data-loom/configs"
	. "github.com/sunkaimr/data-loom/internal/job/status"
	"github.com/sunkaimr/data-loom/internal/middlewares"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	. "github.com/sunkaimr/data-loom/internal/pkg/queue"
	"github.com/sunkaimr/data-loom/internal/services"
	"github.com/sunkaimr/data-loom/internal/workflow"
	"github.com/sunkaimr/data-loom/internal/workflow/types"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"strconv"
	"strings"
	"time"
)

func CleanWorkFlow(ctx *common.Context) {
	ctx.Wg.Add(1)
	defer ctx.Wg.Done()
	log, db := ctx.Log, ctx.DB

	var tasks []models.Task
	err := db.Model(models.Task{}).Select("id, work_flow").
		Where("task_end_time < ? AND task_status IN (?) AND work_flow != ''",
			time.Now().Add(-24*time.Hour*time.Duration(services.Cfg.WorkflowRetentionDays)).Format(time.DateTime),
			common.TaskStatusHasFinished).
		Find(&tasks).Error
	if err != nil {
		log.Errorf("query CleanWorkFlow task failed, %s", err)
		return
	}

	for _, task := range tasks {
		err = workflow.NewDriver(configs.C.WorkFlow.Driver).DeleteWorkFlow(ctx, task.WorkFlow)
		if err != nil {
			log.Errorf("delete task(%v) workflow(%s) failed, %s", task.ID, task.WorkFlow, err)
		} else {
			log.Infof("delete task(%v) workflow(%s) success", task.ID, task.WorkFlow)
		}

		if err == nil || strings.Contains(err.Error(), "not found") {
			db.Model(models.Task{}).Where("id =?", task.ID).Update("work_flow", "")
		}
	}
}

func CallTaskWorkFlowCheck(ctx *common.Context, task *models.Task) error {
	log := ctx.Log
	db := ctx.DB

	ginCtx := middlewares.NewGinContext(log, db)

	orgTask := *task

	defer func() {
		err := db.Save(task).Error
		if err != nil {
			log.Errorf("update models.Task(%v) from db failed, %s", task.ID, err)
		}

		if task.TaskReason != orgTask.TaskReason || task.TaskStatus != orgTask.TaskStatus {
			services.CreateTaskChangeLog(ctx, task, common.SystemUserName,
				fmt.Sprintf(common.TaskChangeLogWaitingExec, task.TaskReason, task.TaskDetail))
		}
	}()

	clusterSvc, err := services.GetClusterServiceByClusterID(ginCtx, task.SrcClusterID)
	if err != nil {
		log.Error(err)
		task.TaskStatus = common.TaskStatusExecCheckFailed
		task.TaskReason = common.CodeServerErr.Message
		task.TaskDetail = err.Error()
		return err
	}

	// 集群磁盘剩余空间
	freeDisk, err := services.NewClusterDriver(clusterSvc).GetFreeDisk(ginCtx)
	if err != nil {
		log.Error(err)
		task.TaskStatus = common.TaskStatusExecCheckFailed
		task.TaskReason = common.CodeClusterFreeDiskErr.Message
		task.TaskDetail = err.Error()
		return err
	}
	task.SrcClusterFreeDisk = freeDisk

	// 清理前表大小
	tableSize, code, err := services.NewClusterDriver(clusterSvc).GetTablesSize(ginCtx, task.SrcDatabaseName, strings.Split(task.SrcTablesName, ","))
	if err != nil {
		log.Error(err)
		task.TaskStatus = common.TaskStatusExecCheckFailed
		task.TaskReason = code.Message
		task.TaskDetail = err.Error()
		return err
	}
	task.SrcClusterSumTableSize = tableSize

	return nil
}

func CallTaskWorkFlow(ctx *common.Context, task *models.Task) error {
	log := ctx.Log
	db := ctx.DB

	orgTask := *task
	defer func() {
		err := db.Save(task).Error
		if err != nil {
			log.Errorf("update models.Task(%v) from db failed, %s", task.ID, err)
		}

		if task.TaskReason != orgTask.TaskReason || task.TaskStatus != orgTask.TaskStatus {
			content := ""
			if task.TaskStatus == common.TaskStatusExecuting {
				content = common.TaskChangeLogCallWorkFlowSuccess
			} else {
				content = fmt.Sprintf(common.TaskChangeLogCallWorkFlowFailed, task.TaskReason, task.TaskDetail)
			}
			services.CreateTaskChangeLog(ctx, task, common.SystemUserName, content)
		}
	}()

	task.TaskStatus = common.TaskStatusExecCheckFailed

	ginCtx := middlewares.NewGinContext(log, db)
	clusterSvc, err := services.GetClusterServiceByClusterID(ginCtx, task.SrcClusterID)
	if err != nil {
		log.Error(err)
		task.TaskReason = common.CodeServerErr.Message
		task.TaskDetail = err.Error()
		return err
	}

	addr, port, user, passwd, code, err := services.NewClusterDriver(clusterSvc).ClusterInfo(ginCtx)
	if err != nil {
		err = fmt.Errorf("get cluster(%s) info failed, %s", clusterSvc.ClusterID, err)
		log.Error(err)
		task.TaskReason = code.Message
		task.TaskDetail = err.Error()
		return err
	}
	task.TaskStartTime = time.Now()

	// 调用工作流
	switch task.Govern {
	case common.GovernTypeTruncate:
		task.WorkFlow, err = workflow.NewDriver(configs.C.WorkFlow.Driver).TruncateData(ctx,
			BuildTruncateDataPara(task, addr, port, user, passwd))
	case common.GovernTypeDelete:
		task.WorkFlow, err = workflow.NewDriver(configs.C.WorkFlow.Driver).DeleteData(ctx,
			BuildDeleteDataPara(task, addr, port, user, passwd))
	case common.GovernTypeArchive:
		task.WorkFlow, err = workflow.NewDriver(configs.C.WorkFlow.Driver).ArchiveData(ctx,
			BuildArchivePara(task, addr, port, user, passwd))
	case common.GovernTypeRebuild:
		task.WorkFlow, err = workflow.NewDriver(configs.C.WorkFlow.Driver).RebuildTables(ctx,
			BuildRebuildDataPara(task, addr, port, user, passwd))
	default:
		err = fmt.Errorf("unsupported task govern(%s)", task.Govern)
		log.Error(err)
		task.TaskStatus = common.TaskStatusExecFailed
		task.TaskReason = common.CodeWorkFlowUnsupported.Message
		task.TaskDetail = err.Error()
		return err
	}
	if err != nil {
		err = fmt.Errorf("call workflow[%s].%s failed, %s", configs.C.WorkFlow.Driver, task.Govern, err)
		log.Error(err)
		task.TaskStatus = common.TaskStatusExecFailed
		task.TaskReason = common.CodeWorkFlowCallFailed.Message
		task.TaskDetail = err.Error()
		return err
	}

	task.TaskStatus = common.TaskStatusExecuting
	task.TaskReason = ""
	task.TaskDetail = ""
	task.TaskStartTime = time.Now()

	return nil
}

func BuildTruncateDataPara(task *models.Task, host, port, user, passwd string) *types.TruncateParaStruct {
	return &types.TruncateParaStruct{
		TaskID:   task.ID,
		Host:     host,
		Port:     port,
		User:     user,
		Password: passwd,
		Database: task.SrcDatabaseName,
		Tables:   task.SrcTablesName,
		Callback: types.Callback{
			URL:   generateCallbackUrl(),
			Token: generateToken(task.ID),
		},
	}
}

func BuildDeleteDataPara(task *models.Task, host, port, user, passwd string) *types.DeleteParaStruct {
	var execWin []string
	_ = json.Unmarshal(task.ExecuteWindow, &execWin)
	if len(execWin) < 2 {
		execWin = append(execWin, "", "")
	}

	return &types.DeleteParaStruct{
		TaskID:      task.ID,
		StartTime:   execWin[0],
		EndTime:     execWin[1],
		Host:        host,
		Port:        port,
		User:        user,
		Password:    passwd,
		Database:    task.SrcDatabaseName,
		Tables:      task.SrcTablesName,
		Condition:   task.Condition,
		RebuildFlag: task.RebuildFlag,
		FreeDisk:    strconv.Itoa(task.SrcClusterFreeDisk),
		Callback: types.Callback{
			URL:   generateCallbackUrl(),
			Token: generateToken(task.ID),
		},
	}
}

func BuildRebuildDataPara(task *models.Task, host, port, user, passwd string) *types.RebuildParaStruct {
	return &types.RebuildParaStruct{
		TaskID:   task.ID,
		Host:     host,
		Port:     port,
		User:     user,
		Password: passwd,
		Database: task.SrcDatabaseName,
		Tables:   task.SrcTablesName,
		FreeDisk: strconv.Itoa(task.SrcClusterFreeDisk),
		Callback: types.Callback{
			URL:   generateCallbackUrl(),
			Token: generateToken(task.ID),
		},
	}
}

func BuildArchivePara(task *models.Task, host, port, user, passwd string) *types.ArchiveParaStruct {
	return &types.ArchiveParaStruct{
		TaskID:   task.ID,
		Host:     host,
		Port:     port,
		User:     user,
		Password: passwd,
		Database: task.SrcDatabaseName,
		Tables:   task.SrcTablesName,
		FreeDisk: strconv.Itoa(task.SrcClusterFreeDisk),
		Callback: types.Callback{
			URL:   generateCallbackUrl(),
			Token: generateToken(task.ID),
		},
	}
}

// 为更新task任务状态单独生成一个token
// 1, 只能调用更新任务状态接口
// 2，每个任务单独的一个token，只能更新本任务
// 3, token有限期30天
func generateToken(taskID uint) string {
	claims := &common.Claims{}
	claims.UserID = taskID
	claims.UserName = common.UpdateTaskResultUser
	claims.RealName = common.UpdateTaskResultUserName
	claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Hour * 24 * 30))
	token, _ := common.GenerateToken(claims)
	return token
}

func generateCallbackUrl() string {
	return configs.C.Server.ExternalAddr + "/data-loom/api/v1/task/result"
}

// CheckWorkFlowTimeout 定时同步任务和workflow的状态
func CheckWorkFlowTimeout(ctx *common.Context) {
	log, db := ctx.Log, ctx.DB
	needScheduleNext := false
	var tasks []models.Task
	err := db.Model(models.Task{}).Where("task_status =?", common.TaskStatusExecuting).Find(&tasks).Error
	if err != nil {
		log.Errorf("query models.Task(task_status=%v) from db failed, %s", common.TaskStatusExecuting, err)
		return
	}

	for _, task := range tasks {
		handle := &TaskQueueHandle{ID: task.ID, PolicyID: task.PolicyID, HandleID: utils.RandStr(20)}
		newCtx := common.NewContext().
			WithContext(ctx.Context).
			WithCancel(ctx.Cancel).
			WithLog(ctx.Log.With(WithExtra[*TaskQueueHandle](handle)...)).
			WithDB(ctx.DB).
			SetData(HandleID, handle.HandleID)
		log := newCtx.Log

		// 再次查询workflow获取状态，忽略Running状态的工作流，成功或失败设置对应任务状态
		status, err := workflow.NewDriver(configs.C.WorkFlow.Driver).WorkFlowStatus(newCtx, task.WorkFlow)
		if err != nil {
			err = fmt.Errorf("get task(%v) workflow(%v) status failed, %s", task.ID, task.WorkFlow, err)
			log.Error(err)
			task.TaskStatus = common.TaskStatusExecFailed
			task.TaskReason = common.CodeWorkFlowGetStatusErr.Message
			task.TaskDetail = err.Error()
			goto update
		}

		switch status {
		case types.WorkFlowStatusPending:
			task.TaskReason = common.CodeWorkFlowPending.Message
			task.TaskDetail = ""
			goto update
		case types.WorkFlowStatusSucceeded:
			task.TaskStatus = common.TaskStatusSuccess
			task.TaskReason = ""
			task.TaskDetail = ""
			needScheduleNext = true
			log.With(ProcExecSuccess).Infof("check task(%s) status is %s", task.Name, task.TaskStatus)
			goto update
		case types.WorkFlowStatusFailed, types.WorkFlowStatusError:
			task.TaskStatus = common.TaskStatusExecFailed
			task.TaskReason = ""
			task.TaskDetail = ""
			needScheduleNext = true
			log.With(ProcExecFailed).Infof("check task(%s) status is %s", task.Name, task.TaskStatus)
			goto update
		case types.WorkFlowStatusUnknown:
			task.TaskStatus = common.TaskStatusExecFailed
			task.TaskReason = common.CodeWorkFlowUnknownStatus.Message
			task.TaskDetail = ""
			needScheduleNext = true
			log.With(ProcExecFailed).Infof("check task(%s) status is %s", task.Name, task.TaskStatus)
			goto update
		}

		// running状态&判断是否超时
		if task.TaskStartTime.Add(time.Second * time.Duration(services.Cfg.TaskTimeout)).After(time.Now()) {
			continue
		}

		// 任务已超时，设置任务的状态为超时
		task.TaskStatus = common.TaskStatusTimeout
		task.TaskReason = ""
		task.TaskDetail = ""
		needScheduleNext = true
		log.With(ProcExecTimeout).Errorf("task(%v) workflow(%v) start(%s) timeout(%v)", task.ID, task.WorkFlow, task.TaskStartTime.Format(time.DateTime), services.Cfg.TaskTimeout)
		goto update

	update:
		// 更新任务状态
		err = updateTaskStatus(newCtx, &task)
		if err != nil {
			log.Errorf("update task(%v) status(%v) failed, %s", task.ID, task.TaskStatus, err)
		}
		// 实例化下一次任务
		if needScheduleNext {
			handleID, ok := newCtx.GetData(HandleID)
			if !ok {
				handleID = utils.RandStr(20)
			}
			handle := &PolicyQueueHandle{ID: task.PolicyID, HandleID: handleID.(string)}
			ok = PolicyQueue.Push(*handle)
			if ok {
				ctx.Log.With(WithExtra[*PolicyQueueHandle](handle, ProcQueued)...).Infof("policy %v push queue sucess", task.PolicyID)
			} else {
				ctx.Log.With(WithExtra[*PolicyQueueHandle](handle, ProcNotQueue)...).Infof("policy %v not push queue", task.PolicyID)
			}
		}
	}
}

func updateTaskStatus(ctx *common.Context, task *models.Task) error {
	ginCtx := middlewares.NewGinContext(ctx.Log, ctx.DB)
	task.TaskEndTime = time.Now()
	task.TaskDuration = int(task.TaskEndTime.Sub(task.TaskStartTime).Seconds())

	clusterSvc, err := services.GetClusterServiceByClusterID(ginCtx, task.SrcClusterID)
	if err != nil {
		return err
	}

	tableSize, _, err := services.NewClusterDriver(clusterSvc).GetTablesSize(ginCtx, task.SrcDatabaseName, strings.Split(task.SrcTablesName, ","))
	if err != nil {
		return err
	}

	task.TaskResultSize = task.SrcClusterSumTableSize - tableSize
	task.TaskResultSize = utils.Ternary[int](task.TaskResultSize > 0, task.TaskResultSize, 0)

	err = ctx.DB.Save(&task).Error
	if err != nil {
		return err
	}

	services.CreateTaskChangeLog(ctx, task, common.SystemUserName, common.TaskChangeLogWorkFlowFinished)
	return nil
}
