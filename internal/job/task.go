package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/robfig/cron/v3"
	"github.com/sunkaimr/data-loom/configs"
	. "github.com/sunkaimr/data-loom/internal/job/status"
	"github.com/sunkaimr/data-loom/internal/job/workflow"
	"github.com/sunkaimr/data-loom/internal/middlewares"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	. "github.com/sunkaimr/data-loom/internal/pkg/queue"
	"github.com/sunkaimr/data-loom/internal/services"
	workflow1 "github.com/sunkaimr/data-loom/internal/workflow"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"gorm.io/gorm"
	"sort"
	"strings"
	"time"
)

// StartTaskJob 工作流相关的定时任务
func StartTaskJob(ctx *common.Context) {
	ctx.Wg.Add(1)
	defer ctx.Wg.Done()
	var err error
	log := ctx.Log

	go PolicyHandle(ctx)
	go TaskHandle(ctx)

	policyEntryID := cron.EntryID(0)
	taskEntryID := cron.EntryID(0)
	taskRecheckEntryID := cron.EntryID(0)
	runWorkflowEntryID := cron.EntryID(0)
	checkWorkflowTimeoutEntryID := cron.EntryID(0)
	checkDiskUsageEntryID := cron.EntryID(0)

	c := cron.New()

	// 一天运行一次：运行策略调度任务，将策略实例化为任务
	policyEntryID, err = c.AddFunc(
		configs.C.Job.PolicyCron,
		func() {
			ctx.Wg.Add(1)
			defer ctx.Wg.Done()

			sTime := time.Now()
			log.Debugf("running CreateOrUpdateTaskByAllPolicies")
			CreateOrUpdateTaskByAllPolicies(ctx)
			log.Debugf("running CreateOrUpdateTaskByAllPolicies done, cost:%v", time.Now().Sub(sTime))
			log.Debugf("next run CreateOrUpdateTaskByAllPolicies at %s", c.Entry(policyEntryID).Next.Format(time.DateTime))
		})
	if err != nil {
		log.Fatalf("add cron job(%s) for CreateOrUpdateTaskByAllPolicies failed %v", configs.C.Job.PolicyCron, err)
	}

	// 一天运行一次：提前一天检查任务是否具备执行条件
	taskEntryID, err = c.AddFunc(
		configs.C.Job.TaskCron,
		func() {
			ctx.Wg.Add(1)
			defer ctx.Wg.Done()

			sTime := time.Now()
			log.Debugf("running CheckScheduledTask")
			CheckScheduledTask(ctx)
			log.Debugf("running CheckScheduledTask done, cost:%v", time.Now().Sub(sTime))
			log.Debugf("next run CheckScheduledTask at %s", c.Entry(taskEntryID).Next.Format(time.DateTime))
		})
	if err != nil {
		log.Fatalf("add cron job(0 * * * *) for ScheduledTask failed %v", err)
	}

	// 每小时一次重新检查填充任务信息失败的任务
	taskRecheckEntryID, err = c.AddFunc(
		"@every 1m",
		func() {
			ctx.Wg.Add(1)
			defer ctx.Wg.Done()

			sTime := time.Now()
			log.Debugf("running RecheckSupplementFailedTask")
			RecheckSupplementFailedTask(ctx)
			log.Debugf("running RecheckSupplementFailedTask done, cost:%v", time.Now().Sub(sTime))
			log.Debugf("next run RecheckSupplementFailedTask at %s", c.Entry(taskRecheckEntryID).Next.Format(time.DateTime))
		})
	if err != nil {
		log.Fatalf("add cron job(@every 1m) for ScheduledTask failed %v", err)
	}

	// 调用具体的工作流
	runWorkflowEntryID, err = c.AddFunc(
		"@every 1m",
		func() {
			ctx.Wg.Add(1)
			defer ctx.Wg.Done()

			sTime := time.Now()
			log.Debugf("running RunTaskWorkFlow")
			RunTaskWorkFlow(ctx)
			log.Debugf("running RunTaskWorkFlow done, cost:%v", time.Now().Sub(sTime))
			log.Debugf("next run RunTaskWorkFlow at %s", c.Entry(runWorkflowEntryID).Next.Format(time.DateTime))
		})
	if err != nil {
		log.Fatalf("add cron job(@every 1m) for RunTaskWorkFlow failed %v", err)
	}

	// 检查工作流执行结果（防止错过回调失败的）
	checkWorkflowTimeoutEntryID, err = c.AddFunc(
		"@every 1m",
		func() {
			ctx.Wg.Add(1)
			defer ctx.Wg.Done()

			sTime := time.Now()
			log.Debugf("running CheckWorkFlowTimeout")
			workflow.CheckWorkFlowTimeout(ctx)
			log.Debugf("running CheckWorkFlowTimeout done, cost:%v", time.Now().Sub(sTime))
			log.Debugf("next run CheckWorkFlowTimeout at %s", c.Entry(checkWorkflowTimeoutEntryID).Next.Format(time.DateTime))
		})
	if err != nil {
		log.Fatalf("add cron job(@every 1m) for RunTaskWorkFlow failed %v", err)
	}

	// 检查磁盘使用率，超过设定值自动停止工作流
	checkDiskUsageEntryID, err = c.AddFunc(
		"@every 10s",
		func() {
			ctx.Wg.Add(1)
			defer ctx.Wg.Done()

			sTime := time.Now()
			log.Debugf("running CheckDiskUsage")
			CheckDiskUsage(ctx)
			log.Debugf("running CheckDiskUsage done, cost:%v", time.Now().Sub(sTime))
			log.Debugf("next run CheckDiskUsage at %s", c.Entry(checkDiskUsageEntryID).Next.Format(time.DateTime))
		})
	if err != nil {
		log.Fatalf("add cron job(@every 10s) for RunTaskWorkFlow failed %v", err)
	}

	c.Start()
	log.Debugf("next run CreateOrUpdateTaskByAllPolicies at %s", c.Entry(policyEntryID).Next.Format(time.DateTime))
	log.Debugf("next run CheckScheduledTask at %s", c.Entry(taskEntryID).Next.Format(time.DateTime))

	<-ctx.Context.Done()
	c.Stop()
	log.Info("shutdown cron job")
}

func PolicyHandle(ctx *common.Context) {
	ctx.Wg.Add(1)
	defer ctx.Wg.Done()

	tick := time.NewTicker(time.Millisecond * 100)
	for {
		select {
		case <-ctx.Context.Done():
			ctx.Log.Info("shutdown PolicyHandle")
			return
		case <-tick.C:
			v, ok := PolicyQueue.Pop()
			if !ok {
				continue
			}
			handle := any(v).(PolicyQueueHandle)

			newCtx := common.NewContext().
				WithContext(ctx.Context).
				WithCancel(ctx.Cancel).
				WithLog(ctx.Log.With(WithExtra[*PolicyQueueHandle](&handle)...)).
				WithDB(ctx.DB)
			newCtx.SetData(HandleID, handle.HandleID)
			newCtx.Log.With(ProcDequeue).Infof("policy(%v) out of queue", handle.ID)
			AttemptCreateOrUpdateTask(newCtx, v.ID)
		}
	}
}

func TaskHandle(ctx *common.Context) {
	ctx.Wg.Add(1)
	defer ctx.Wg.Done()

	tick := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Context.Done():
			ctx.Log.Info("shutdown TaskHandle")
			return
		case <-tick.C:
			v, ok := TaskQueue.Pop()
			if !ok {
				continue
			}

			handle := any(v).(TaskQueueHandle)

			newCtx := common.NewContext().
				WithContext(ctx.Context).
				WithCancel(ctx.Cancel).
				WithLog(ctx.Log.With(WithExtra[*TaskQueueHandle](&handle)...)).
				WithDB(ctx.DB)

			newCtx.SetData(HandleID, handle.HandleID)
			newCtx.Log.With(ProcDequeue).Infof("task(%v) out of queue", handle.ID)
			err := SupplementaryTaskInformation(newCtx, v.ID)
			if err != nil {
				newCtx.Log.With(ProcSupplementFailed).Errorf("supplementary task(%v) information failed, %s", v.ID, err)
			} else {
				newCtx.Log.With(ProcSupplemented).Infof("supplementary task(%v) information", v.ID)
			}
		}
	}
}

// CreateOrUpdateTaskByAllPolicies 进行任务的创建和更新；1，每天定时检查一次，2，在创建策略、修改策略、任务执行完需主动调用
func CreateOrUpdateTaskByAllPolicies(ctx *common.Context) {
	log := ctx.Log
	db := ctx.DB

	var policies []models.Policy
	err := db.Model(models.Policy{}).Select("id, name").Where("enable =?", true).Find(&policies).Error
	if err != nil {
		log.Errorf("query models.Policy(enable=true) from db failed, %s", err)
		return
	}
	for _, policy := range policies {
		handle := &PolicyQueueHandle{ID: policy.ID, HandleID: utils.RandStr(20)}

		log.With(WithExtra[*PolicyQueueHandle](handle, ProcQueuing)...).Infof("policy %s push queue...", policy.Name)

		ok := PolicyQueue.Push(*handle)
		if ok {
			log.With(WithExtra[*PolicyQueueHandle](handle, ProcQueued)...).Infof("policy %s push queue sucess", policy.Name)
		} else {
			log.With(WithExtra[*PolicyQueueHandle](handle, ProcNotQueue)...).Infof("policy %s not push queue", policy.Name)
		}
	}
}

func RecheckSupplementFailedTask(ctx *common.Context) {
	log, db := ctx.Log, ctx.DB

	var tasks []models.Task
	err := db.Model(models.Task{}).Where("enable =? AND task_status IN (?)", true, common.TaskStatusSupplementFailed).Find(&tasks).Error
	if err != nil {
		log.Errorf("query models.Task(enable=true AND task_status IN (%v)) from db failed, %s", common.TaskStatusSupplementFailed, err)
		return
	}
	for _, task := range tasks {
		// 重新检查补充任务信息失败的任务
		handle := &TaskQueueHandle{ID: task.ID, PolicyID: task.PolicyID, HandleID: utils.RandStr(20)}

		log.With(WithExtra[*TaskQueueHandle](handle, ProcQueuing)...).Infof("task %s push queue...", task.Name)

		ok := TaskQueue.Push(*handle)
		if ok {
			log.With(WithExtra[*TaskQueueHandle](handle, ProcQueued)...).Infof("task %s push queue sucess", task.Name)
		} else {
			log.With(WithExtra[*TaskQueueHandle](handle, ProcNotQueue)...).Infof("task %s not push queue", task.Name)
		}
	}
}

func RunTaskWorkFlow(ctx *common.Context) {
	log, db := ctx.Log, ctx.DB

	services.Cfg.ReloadConfig(middlewares.NewGinContext(log, db))

	var tasks []models.Task
	err := db.Model(models.Task{}).Where("enable =? AND task_status IN(?)", true, common.TaskStatusCanExec).Find(&tasks).Error
	if err != nil {
		log.Errorf("query models.Task(enable=%v AND task_status=%v) from db failed, %s", true, common.TaskStatusCanExec, err)
		return
	}

	type admission func(*common.Context, *models.Task) (bool, common.ServiceCode, error)
	admissionFunc := []admission{
		TaskExecDateAdmission,     // 达到执行日期
		TaskInExecWindowAdmission, // 任务是否在执行窗口
		TaskParallelAdmission,     // 最大并发校验
		TaskConflictAdmission,     // 判断和当前任务的源集群是否冲突
	}

	var filterTasks []models.Task
	for i, task := range tasks {
		handle := &TaskQueueHandle{ID: task.ID, PolicyID: task.PolicyID, HandleID: utils.RandStr(20)}
		newCtx := common.NewContext().
			WithContext(ctx.Context).
			WithCancel(ctx.Cancel).
			WithLog(ctx.Log.With(WithExtra[*TaskQueueHandle](handle)...)).
			WithDB(ctx.DB)

		pass := true
		for n, f := range admissionFunc {
			ok, code, err := f(newCtx, &task)
			if !ok || err != nil {
				pass = false
				task.TaskReason = code.Message
				task.TaskDetail = fmt.Sprintf("%v", err)
				newCtx.Log.Info(err)
				break
			}
			log.Infof("task(%v) pass admission step(%d/%d) check", task.Name, n+1, len(admissionFunc))
		}

		// 准入检查未通过，更新任务状态
		if !pass {
			err = db.Save(&task).Error
			if err != nil {
				log.Errorf("update models.Task(ID=%v) from db failed, %s", task.ID, err)
			}

			if task.TaskReason != tasks[i].TaskReason || task.TaskStatus != tasks[i].TaskStatus {
				services.CreateTaskChangeLog(ctx, &task, common.SystemUserName,
					fmt.Sprintf(common.TaskChangeLogWaitingExec, task.TaskReason, task.TaskDetail))
			}
			continue
		}
		newCtx.Log.Infof("task(%v) passed pre execution dmission check", task.Name)
		filterTasks = append(filterTasks, tasks[i])
	}

	// 排序
	sort.Slice(filterTasks, func(i, j int) bool {
		// 执行窗口
		res, err := compareExecWindows(filterTasks[i].ExecuteWindow.String(), filterTasks[j].ExecuteWindow.String())
		if err != nil {
			log.Errorf("compare exec windows task1:(%v) task2:(%v) failed, %s", filterTasks[i].ExecuteWindow.String(), filterTasks[j].ExecuteWindow.String(), err)
			return false
		}
		switch {
		case res < 0:
			return true
		case res > 0:
			return false
		}

		// 任务创建时间
		createTime1 := filterTasks[i].CreatedAt.Format(time.DateOnly)
		createTime2 := filterTasks[j].CreatedAt.Format(time.DateOnly)
		switch {
		case createTime1 < createTime2:
			return true
		case createTime1 > createTime2:
			return false
		}

		// 等待执行 > 检查失败
		if filterTasks[i].TaskStatus == common.TaskStatusWaiting && filterTasks[j].TaskStatus == common.TaskStatusWaiting {
			if filterTasks[i].ID < filterTasks[j].ID {
				return true
			}
		}
		return false
	})

	for i, task := range filterTasks {
		handle := &TaskQueueHandle{ID: task.ID, PolicyID: task.PolicyID, HandleID: utils.RandStr(20)}
		newCtx := common.NewContext().
			WithContext(ctx.Context).
			WithCancel(ctx.Cancel).
			WithLog(ctx.Log.With(WithExtra[*TaskQueueHandle](handle)...)).
			WithDB(ctx.DB)
		log := newCtx.Log

		// 再次进行最大并发检验
		executingTask, err := GetExecutingTaskNum(ctx)
		if err != nil {
			log.Errorf("query models.Task(task_status IN (%v)) from db failed, %s", common.TaskStatusExecuting, err)
			return
		}

		if executingTask >= GetMaxParallel() {
			err = fmt.Errorf("current running task num(%v) has reached the maximum parallel limit(%v)", executingTask, GetMaxParallel())
			log.Error(err)
			task.TaskReason = common.CodeTaskParallelUpperLimit.Message
			task.TaskDetail = err.Error()
			err = db.Save(&task).Error
			if err != nil {
				log.Errorf("update models.Task(ID=%v) from db failed, %s", task.ID, err)
			}

			if task.TaskReason != filterTasks[i].TaskReason || task.TaskStatus != filterTasks[i].TaskStatus {
				services.CreateTaskChangeLog(ctx, &task, common.SystemUserName,
					fmt.Sprintf(common.TaskChangeLogWaitingExec, task.TaskReason, task.TaskDetail))
			}
			continue
		}

		// 再次进行任务冲突检验
		ok, code, err := TaskConflictAdmission(newCtx, &task)
		if !ok || err != nil {
			log.Error(err)
			task.TaskReason = code.Message
			task.TaskDetail = err.Error()
			err = db.Save(&task).Error
			if err != nil {
				log.Errorf("update models.Task(ID=%v) from db failed, %s", task.ID, err)
			}

			if task.TaskReason != filterTasks[i].TaskReason || task.TaskStatus != filterTasks[i].TaskStatus {
				services.CreateTaskChangeLog(ctx, &task, common.SystemUserName,
					fmt.Sprintf(common.TaskChangeLogWaitingExec, task.TaskReason, task.TaskDetail))
			}
			continue
		}

		// 调用工作流前的检查
		err = workflow.CallTaskWorkFlowCheck(newCtx, &task)
		if err != nil {
			log.With(ProcExecCheckFailed).Errorf("task(%v) exec check failed, %s", task.Name, err)
			continue
		}
		log.With(ProcExecCheckOk).Infof("task(%v) exec check success", task.Name)

		// 调用工作流
		err = workflow.CallTaskWorkFlow(newCtx, &task)
		if err != nil {
			log.With(ProcCallWorkflowFailed).Errorf("task(%v) call workflow failed, %s", task.Name, err)
		} else {
			log.With(ProcExecuting).Infof("call task(%v) workflow success", task.Name)
		}

		// 一次性任务：实例化任务后策略改为disable
		err = setPeriodOnceTaskDisable(ctx, &task)
		if err != nil {
			log.Errorf("set period once task(%v) disable failed, %s", task.ID, err)
		}
	}
}

func JudgeCouldCreateTask(ctx *common.Context, policyID uint) (bool, error) {
	var count int64

	var policy models.Policy
	err := ctx.DB.Model(models.Policy{}).Where("id=?", policyID).First(&policy).Error
	if err != nil {
		return false, fmt.Errorf("query models.Policy(id=%v) from db failed, %s", policyID, err)
	}

	if !policy.Enable {
		return false, nil
	}

	err = ctx.DB.Model(models.Task{}).Where("policy_id=? AND task_status NOT IN (?)",
		policyID, common.TaskStatusHasFinished).Count(&count).Error
	if err != nil {
		return false, fmt.Errorf("query models.Task(policy_id=%v AND task_status NOT IN (%v)) from db failed, %s",
			policyID, common.TaskStatusHasFinished, err)
	}
	return count == 0, nil
}

func JudgeCouldUpdateTask(ctx *common.Context, policyID uint) (bool, error) {
	var count int64
	err := ctx.DB.Model(models.Task{}).Where("policy_id =? AND task_status IN (?)",
		policyID, common.TaskStatusCanUpdate).Count(&count).Error
	if err != nil {
		return false, fmt.Errorf("query models.Task(policy_id=%v AND task_status IN (%v)) from db failed, %s",
			policyID, common.TaskStatusCanUpdate, err)
	}
	return count != 0, nil
}

// 是否错过本月的执行日期
func missedExecDateAndWin(policy *models.Policy) (bool, error) {
	switch policy.Period {
	case common.PeriodMonthly, common.PeriodQuarterly, common.PeriodSixMonths, common.PeriodYearly:
		if time.Now().Day() > policy.Day {
			return true, nil
		} else if time.Now().Day() == policy.Day {
			// 判断是否错过当天执行窗口
			if b, err := timeInExecWindows(time.Now().Format(time.DateOnly), time.Now(), policy.ExecuteWindow); err != nil {
				return false, fmt.Errorf("judge time(%s) in exec windows(%s) failed, %s",
					time.Now().Format(time.DateTime), string(policy.ExecuteWindow), err)
			} else if b > 0 {
				// 已经错过了执行窗口
				return true, nil
			}
		}
	default:
		// 判断是否错过当天执行窗口
		if b, err := timeInExecWindows(time.Now().Format(time.DateOnly), time.Now(), policy.ExecuteWindow); err != nil {
			return false, fmt.Errorf("judge time(%s) in exec windows(%s) failed, %s",
				time.Now().Format(time.DateTime), string(policy.ExecuteWindow), err)
		} else if b > 0 {
			// 已经错过了执行窗口
			return true, nil
		}
	}

	return false, nil
}

// CreateTask 首次创建策略&任务执行完毕需要重新生成任务
func CreateTask(ctx *common.Context, policy *models.Policy) (*models.Task, error) {
	task := &models.Task{}

	// 对于此前已经执行过任务的策略从上次执行日期开始往后就算一个周期
	lastExecDate, err := GetPolicyActualLastExecDate(ctx, policy.ID)
	if err != nil {
		return task, fmt.Errorf("get policy(%v) actual last execute date failed, %s", policy.ID, err)
	}

	source := &models.Source{}
	err = ctx.DB.Model(models.Source{}).Where("id =?", policy.SrcID).First(source).Error
	if err != nil {
		return task, fmt.Errorf("query models.Source(id=%v) from db failed, %s", policy.SrcID, err)
	}

	// 如果lastExecDate.IsZero()代表此前已经执行过任务(一次性任务除外)
	// 此时会根据上次任务的ExecDate日期向后延长一个周期
	if !lastExecDate.IsZero() && policy.Period != common.PeriodOnce {
		executeTime := policyNextScheduleTime(policy, lastExecDate)
		task.ExecuteDate = formatExecDate(policy, executeTime)
		task = generateEmptyTaskByPolicy(task, policy, source)
		return task, nil
	}

	// 对于新创建的策略此前未执行过任务的：分2种情况
	// 1, 对于清理频率是月度以下的（忽略day）: 只要判断是否错过执行窗口如果错过设置下一天执行，如果没错过设置当天执行
	// 2, 对于清理频率是月度以上的：
	//    a): day > 当前日期 顺延一个周期
	//    b): 已经错过了执行窗口 顺延一个周期
	//    c): day == 当前日期 && 没有错过执行窗口 直接设置当天执行
	missedExecWin, err := missedExecDateAndWin(policy)
	if err != nil {
		return task, fmt.Errorf("judge policy missed ExecDate and ExecWindow failed, %s", err)
	}

	executeTime := time.Time{}
	if missedExecWin {
		executeTime = policyNextScheduleTime(policy, time.Now())
	} else {
		executeTime = time.Now()
	}
	task.ExecuteDate = formatExecDate(policy, executeTime)
	task = generateEmptyTaskByPolicy(task, policy, source)

	return task, nil
}

// calculateTaskExecDate 计算任务的执行日期判断是否需要更新执行日期
// 修改策略时可能会修改day和period、day、execute_window字段从而导致ExecuteDate发生变化，此时需要更新ExecuteDate和execute_window
func calculateTaskExecDate(ctx *common.Context, policy *models.Policy, task *models.Task) (bool /* 执行日期是否变化 */, error) {
	var err error
	lastExecDate := time.Time{}
	newExecDate := ""

	// 对于一次性任务上一次任务的执行日期不应该影响本次任务的执行日期，所以上一次执行日期给空
	if policy.Period != common.PeriodOnce {
		// 拿到策略上一次任务的计划执行日期
		lastExecDate, err = GetPolicyActualLastExecDate(ctx, policy.ID)
		if err != nil {
			return false, fmt.Errorf("get policy(%v) actual last execute date failed, %s", policy.ID, err)
		}
	}

	if !lastExecDate.IsZero() {
		// 根据策略的上次期望执行日期计算下一次的预期执行日期
		newExecDate = formatExecDate(policy, policyNextScheduleTime(policy, lastExecDate))
	} else {
		// 如果策略还没有执行过任务此时返回lastExecDate.IsZero()对于这种情况重新计算执行日期
		missedExecWin, err := missedExecDateAndWin(policy)
		if err != nil {
			return false, fmt.Errorf("judge policy missed ExecDate and ExecWindow failed, %s", err)
		}
		executeTime := time.Time{}
		if missedExecWin {
			executeTime = policyNextScheduleTime(policy, time.Now())
		} else {
			executeTime = time.Now()
		}
		newExecDate = formatExecDate(policy, executeTime)
	}

	// 如果已经错过了执行窗口往后再延一个周期
	missedExecWin := false
	if b, err := timeInExecWindows(newExecDate, time.Now(), policy.ExecuteWindow); err != nil {
		return false, fmt.Errorf("judge execDate(%s) time(%s) in exec windows(%s) failed, %s",
			newExecDate, time.Now().Format(time.DateTime), string(policy.ExecuteWindow), err)
	} else if b > 0 {
		// 已经错过了执行窗口
		missedExecWin = true
	}

	// 再次顺延一个周期？还是以当前的时间先为基准？
	if missedExecWin {
		// 方式一：顺延2个周期
		//nextExecTime, _ := time.ParseInLocation(time.DateOnly, newExecDate, time.Now().Location())
		//newExecDate = formatExecDate(policy, policyNextScheduleTime(&policy, nextExecTime))

		// 方式二：以当前年月为基准顺延个周期
		newExecDate = formatExecDate(policy, policyNextScheduleTime(policy, time.Now()))
	}

	if task.ExecuteDate != newExecDate || string(task.ExecuteWindow) != string(policy.ExecuteWindow) {
		task.TaskStatus = common.TaskStatusScheduled
		task.ExecuteDate = newExecDate
		task.ExecuteWindow = policy.ExecuteWindow
		return true, nil
	}
	return false, nil
}

// calculateNeedUpdateTaskInfo 策略修改后同步更新任务
func calculateNeedUpdateTaskInfo(_ *common.Context, policy *models.Policy, task *models.Task) (bool /* 策略修改后是否需要更新任务*/, error) {
	if policy.Condition != task.Condition ||
		policy.Pause != task.Pause ||
		policy.RebuildFlag != task.RebuildFlag ||
		policy.CleaningSpeed != task.CleaningSpeed ||
		policy.RetainSrcData != task.RetainSrcData ||
		policy.ArchiveScope != task.SrcColumns ||
		policy.NotifyPolicy != task.NotifyPolicy ||
		string(policy.Relevant) != string(task.Relevant) {

		// 更新对应任务字段
		task.Condition = policy.Condition
		task.Pause = policy.Pause
		task.RebuildFlag = policy.RebuildFlag
		task.CleaningSpeed = policy.CleaningSpeed
		task.RetainSrcData = policy.RetainSrcData
		task.SrcColumns = policy.ArchiveScope
		task.NotifyPolicy = policy.NotifyPolicy
		task.Relevant = policy.Relevant
		return true, nil
	}
	return false, nil
}

func generateEmptyTaskByPolicy(task *models.Task, policy *models.Policy, source *models.Source) *models.Task {
	task.Creator = policy.Creator
	task.Name = fmt.Sprintf("%s-%s", common.PeriodCN[policy.Period], policy.Name)
	task.Enable = policy.Enable
	task.PolicyID = policy.ID
	task.ExecuteWindow = policy.ExecuteWindow

	task.TaskStartTime = time.UnixMilli(0)
	task.TaskEndTime = time.UnixMilli(0)
	task.Pause = policy.Pause
	task.RebuildFlag = policy.RebuildFlag
	task.TaskStatus = common.TaskStatusScheduled
	task.TaskReason = ""
	task.TaskDetail = ""

	// 源端信息
	task.SrcID = policy.SrcID
	task.SrcName = source.Name
	task.SrcBu = source.Bu
	task.SrcClusterID = source.ClusterID
	task.SrcClusterName = source.ClusterName
	task.SrcDatabaseName = source.DatabaseName
	task.SrcTablesName = source.TablesName
	task.SrcColumns = policy.ArchiveScope

	// 数据清理方式
	task.Govern = policy.Govern
	task.Condition = policy.Condition
	task.RetainSrcData = policy.RetainSrcData
	task.CleaningSpeed = policy.CleaningSpeed

	task.Relevant = policy.Relevant
	task.NotifyPolicy = policy.NotifyPolicy
	return task
}

func policyNextScheduleTime(policy *models.Policy, lastExecDate time.Time) time.Time /*仅精确到天*/ {
	if lastExecDate.IsZero() {
		return time.Now()
	}
	switch policy.Period {
	case common.PeriodDay:
		return lastExecDate.Add(time.Hour * 24)
	case common.PeriodTwoDay:
		return lastExecDate.Add(time.Hour * 24 * 2)
	case common.PeriodWeekly:
		return lastExecDate.Add(time.Hour * 24 * 7)
	case common.PeriodTwoWeeks:
		return lastExecDate.Add(time.Hour * 24 * 14)
	case common.PeriodOnce:
		return lastExecDate.Add(time.Hour * 24)
	case common.PeriodMonthly:
		return lastExecDate.AddDate(0, 1, 0)
	case common.PeriodQuarterly:
		return lastExecDate.AddDate(0, 3, 0)
	case common.PeriodSixMonths:
		return lastExecDate.AddDate(0, 6, 0)
	case common.PeriodYearly:
		return lastExecDate.AddDate(1, 0, 0)
	default:
		return time.Now()
	}
}

func timeInExecWindows(execDate string, execTime time.Time, execWindow []byte) (int, error) {
	// 没有到执行日期
	if execDate > execTime.Format(time.DateOnly) {
		return -1, nil
	}

	var executeWin []string
	err := json.Unmarshal(execWindow, &executeWin)
	if err != nil {
		return 0, fmt.Errorf("unmarshal execWindow(%s) failed, %s", string(execWindow), err)
	}
	// 防止panic
	if len(executeWin) < 2 {
		executeWin = append(executeWin, "")
	}

	startTime, err := time.ParseInLocation(time.DateTime, execTime.Format("2006-01-02 ")+executeWin[0], time.Now().Location())
	if err != nil {
		return 0, fmt.Errorf("parse execWindow(%s) failed, %s", string(execWindow), err)
	}
	endTime, err := time.ParseInLocation(time.DateTime, execTime.Format("2006-01-02 ")+executeWin[1], time.Now().Location())
	if err != nil {
		return 0, fmt.Errorf("parse execWindow(%s) failed, %s", string(execWindow), err)
	}

	// 情况一：结束时间大于开始时间：没有跨天，比如：02:00:00 - 06:00:00

	// 情况二：结束时间小于等于开始时间： 跨天了，比如：22:00:00 - 05:00:00
	if executeWin[1] <= executeWin[0] {
		if execDate == execTime.Format(time.DateOnly) {
			endTime = endTime.Add(time.Hour * 24)
		} else {
			startTime = startTime.Add(time.Hour * -24)
		}
	}

	if execTime.Before(startTime) {
		// 在执行窗口之前
		return -1, nil
	} else if execTime.After(endTime) {
		// 在执行窗口之后
		return 1, nil
	} else {
		// 刚好在执行窗口
		return 0, nil
	}
}

func execWindowsToTime(execWindow []byte) ([2]time.Time, error) {
	var executeWinTime [2]time.Time

	var executeWinStr []string
	err := json.Unmarshal(execWindow, &executeWinStr)
	if err != nil {
		return executeWinTime, fmt.Errorf("unmarshal execWindow(%s) failed, %s", string(execWindow), err)
	}
	// 防止panic
	if len(executeWinStr) < 2 {
		executeWinStr = append(executeWinStr, "")
	}

	startTime, err := time.ParseInLocation(time.TimeOnly, executeWinStr[0], time.Now().Location())
	if err != nil {
		return executeWinTime, fmt.Errorf("parse execWindow(%s) failed, %s", string(execWindow), err)
	}
	endTime, err := time.ParseInLocation(time.TimeOnly, executeWinStr[1], time.Now().Location())
	if err != nil {
		return executeWinTime, fmt.Errorf("parse execWindow(%s) failed, %s", string(execWindow), err)
	}

	// 跨天了 endTime < startTime
	if endTime.Before(startTime) {
		endTime = endTime.Add(time.Hour * 24)
	}
	executeWinTime[0] = startTime
	executeWinTime[1] = endTime
	return executeWinTime, nil
}

// 比较execWin1和execWin2的时间先后
// -1：execWin1早于execWin2
// 0：execWin1相同execWin2
// 1：execWin1晚于execWin2
func compareExecWindows(execWin1, execWin2 string) (int, error) {
	execTime1, err1 := execWindowsToTime([]byte(execWin1))
	if err1 != nil {
		return 0, fmt.Errorf("compare exec windows failed, %s", err1)
	}
	execTime2, err2 := execWindowsToTime([]byte(execWin2))
	if err2 != nil {
		return 0, fmt.Errorf("compare exec windows failed, %s", err2)
	}

	if execTime1[0] == execTime2[0] {
		return 0, nil
	}

	if execTime1[0].Before(execTime2[0]) {
		return -1, nil
	}
	return 1, nil
}

func formatExecDate(policy *models.Policy, executeTime time.Time) string {
	switch policy.Period {
	case common.PeriodMonthly, common.PeriodQuarterly, common.PeriodSixMonths, common.PeriodYearly:
		maxDay := utils.DaysInMonth(executeTime.Year(), int(executeTime.Month()))
		if policy.Day > maxDay {
			return fmt.Sprintf("%d-%02d-%02d", executeTime.Year(), executeTime.Month(), maxDay)
		}
		return fmt.Sprintf("%d-%02d-%02d", executeTime.Year(), executeTime.Month(), policy.Day)
	default:
		return executeTime.Format(time.DateOnly)
	}
}

func TaskInExecWindowAdmission(_ *common.Context, task *models.Task) (bool, common.ServiceCode, error) {
	// 判断任务是否在执行窗口 && 剩余窗口是否支持任务执行完毕
	inWin, err := timeInExecWindows(task.ExecuteDate, time.Now(), task.ExecuteWindow)
	if err != nil {
		return false, common.CodeServerErr, fmt.Errorf("judge task(%v) in exec windows %s failed, %s", task.Name, string(task.ExecuteWindow), err)
	}

	switch inWin {
	case -1:
		// 在执行窗口之前
		return false, common.CodeTaskNotReachedExecWin, fmt.Errorf("task has not yet reached the execution time window%s", string(task.ExecuteWindow))
	case 1:
		// 在执行窗口之后
		return false, common.CodeTaskMissedExecWin, fmt.Errorf("task missed exec windows%s", string(task.ExecuteWindow))
	}
	return true, common.CodeOK, nil
}

func CheckSource(ctx *common.Context, source *models.Source) (bool, common.ServiceCode, error) {
	ginCtx := middlewares.NewGinContext(ctx.Log, ctx.DB)

	clusterSvc, err := services.GetClusterServiceByClusterID(ginCtx, source.ClusterID)
	if err != nil {
		return false, common.CodeServerErr, fmt.Errorf("get cluster_id(%s) failed, %s", source.ClusterID, err)
	}

	_, err = services.NewClusterDriver(clusterSvc).GetFreeDisk(ginCtx)
	if err != nil {
		return false, common.CodeClusterFreeDiskErr, fmt.Errorf("check cluster(%s) free disk no pass, %s", clusterSvc.ClusterID, err)
	}

	// 库名存在
	databases, res, err := services.NewClusterDriver(clusterSvc).GetDatabases(ginCtx)
	if err != nil {
		return false, res, fmt.Errorf("get cluster(%s) databases failed, %s", clusterSvc.ClusterID, err)
	}

	if !utils.ElementExist(source.DatabaseName, databases) {
		return false, common.CodeSourceDatabaseNotExist, fmt.Errorf("got database(%v) not in reality cluster(%v)", source.DatabaseName, databases)
	}

	reqTables := strings.Split(source.TablesName, ",")

	// 表存在
	tables, code, err := services.NewClusterDriver(clusterSvc).GetTables(ginCtx, source.DatabaseName)
	if err != nil {
		return false, code, fmt.Errorf("get cluster(%s) databases(%s) tabels failed", clusterSvc.ClusterID, source.DatabaseName)
	}

	if !utils.IsSubSlices(reqTables, tables) {
		return false,
			common.CodeSourceTableNotExist,
			fmt.Errorf("got tables(%v) not in reality cluster(%s) tables(%v)", reqTables, clusterSvc.ClusterID, tables)
	}

	// 检查表是否都有主键
	_, code, err = services.NewClusterDriver(clusterSvc).TablesHasPrimaryKey(ginCtx, source.DatabaseName, reqTables)
	if err != nil {
		return false, code, fmt.Errorf("common cluster(%s) databases(%s) primary key no pass, %s", clusterSvc.ClusterID, source.DatabaseName, err)
	}
	return true, common.CodeOK, nil
}

func CheckDestination(ctx *common.Context, destModel *models.Destination) (bool, common.ServiceCode, error) {
	ginCtx := middlewares.NewGinContext(ctx.Log, ctx.DB)

	dest := &services.DestService{}
	dest.ModelToService(destModel)
	_, res, err := dest.CheckParameters(ginCtx)
	if err != nil {
		return false, res, fmt.Errorf("common Destination(%s) no pass, %s", dest.Name, err)
	}

	return true, common.CodeOK, nil
}

func CheckPolicyCondition(ctx *common.Context, source *models.Source, policy *models.Policy) (bool, common.ServiceCode, error) {
	ginCtx := middlewares.NewGinContext(ctx.Log, ctx.DB)

	clusterSvc, err := services.GetClusterServiceByClusterID(ginCtx, source.ClusterID)
	if err != nil {
		return false, common.CodeServerErr, fmt.Errorf("get cluster_id(%s) failed, %s", source.ClusterID, err)
	}

	tableNames := utils.RemoveSubSlices(strings.Split(source.TablesName, ","), []string{""})
	_, code, err := services.NewClusterDriver(clusterSvc).SQLExplain(ginCtx, source.DatabaseName, tableNames[0], policy.ArchiveScope, policy.Condition)
	if err != nil {
		return false, code, fmt.Errorf("common policy condition not pass, %s", err)
	}

	return true, common.CodeOK, nil
}

func GetMaxParallel() int {
	return services.Cfg.TaskMaxParallel
}

// TaskExecDateAdmission 达到执行日期
func TaskExecDateAdmission(_ *common.Context, task *models.Task) (bool, common.ServiceCode, error) {
	if task.ExecuteDate <= time.Now().Format(time.DateOnly) {
		return true, common.CodeOK, nil
	}
	return false, common.CodeTaskExecDateNotReached, fmt.Errorf("task.ExecuteDate(%s) > now(%v)",
		task.ExecuteDate, time.Now().Format(time.DateOnly))
}

// TaskParallelAdmission 任务最大并发度检查
func TaskParallelAdmission(ctx *common.Context, _ *models.Task) (bool, common.ServiceCode, error) {
	// 最大并发判断
	var count int64
	err := ctx.DB.Model(models.Task{}).Where("task_status IN (?)", common.TaskStatusExecuting).Count(&count).Error
	if err != nil {
		return false, common.CodeServerErr,
			fmt.Errorf("query models.Task(task_status IN (%v)) from db failed, %s", common.TaskStatusExecuting, err)
	}

	if int(count) >= GetMaxParallel() {
		return false, common.CodeTaskParallelUpperLimit,
			fmt.Errorf("current running task num(%v) has reached the maximum parallel limit(%v)", count, GetMaxParallel())
	}
	return true, common.CodeOK, nil
}

func TaskConflictAdmission(ctx *common.Context, task *models.Task) (bool, common.ServiceCode, error) {
	switch services.Cfg.TaskConflictLevel {
	case common.TaskConflictLevelCluster:
		return TaskSrcClusterConflictAdmission(ctx, task)
	case common.TaskConflictLevelDatabase:
		return TaskSrcDatabaseConflictAdmission(ctx, task)
	case common.TaskConflictLevelTable:
		return TaskSrcTableConflictAdmission(ctx, task)
	}
	return false, common.CodeConfigConflictLevelErr, fmt.Errorf("not support task conflict level: %s", services.Cfg.TaskConflictLevel)
}

func TaskSrcClusterConflictAdmission(ctx *common.Context, task *models.Task) (bool, common.ServiceCode, error) {
	// 拿到正在执行的任务列表，判断和当前任务的源是否冲突
	clusterIds, err := GetExecutingTaskClusterID(ctx)
	if err != nil {
		return false, common.CodeServerErr, fmt.Errorf("get executing task src cluster failed, %s", err)
	}

	m := make(map[string]int, len(clusterIds))
	m[task.SrcClusterID] = 1
	for _, k := range clusterIds {
		m[k] = m[k] + 1
		if m[k] > services.Cfg.TaskConflictMax {
			return false, common.CodeTaskSrcClusterConflict,
				fmt.Errorf("source cluster(%v) reached the max num(%v) executing task", task.SrcClusterID, services.Cfg.TaskConflictMax)
		}
	}
	return true, common.CodeOK, nil
}

func TaskSrcDatabaseConflictAdmission(ctx *common.Context, task *models.Task) (bool, common.ServiceCode, error) {
	// 拿到正在执行的任务列表，判断和当前任务的源是否冲突
	databases, err := GetExecutingTaskDatabases(ctx)
	if err != nil {
		return false, common.CodeServerErr, fmt.Errorf("get executing task src database failed, %s", err)
	}

	m := make(map[string]int, len(databases))
	m[task.SrcClusterID+"/"+task.SrcDatabaseName] = 1
	for _, k := range databases {
		m[k] = m[k] + 1
		if m[k] > services.Cfg.TaskConflictMax {
			return false, common.CodeTaskSrcDatabaseConflict, fmt.Errorf("source database(%v) reached the max num(%v) executing task",
				task.SrcClusterID+"/"+task.SrcDatabaseName, services.Cfg.TaskConflictMax)
		}
	}
	return true, common.CodeOK, nil
}

func TaskSrcTableConflictAdmission(ctx *common.Context, task *models.Task) (bool, common.ServiceCode, error) {
	// 拿到正在执行的任务列表，判断和当前任务的源是否冲突
	tables1, err := GetExecutingTaskTables(ctx)
	if err != nil {
		return false, common.CodeServerErr, fmt.Errorf("get executing task src table failed, %s", err)
	}

	_, baseName, err := common.CheckSameShardingTables(task.SrcTablesName)
	if err != nil {
		return false, common.CodeServerErr, fmt.Errorf("get executing task src same sharding tables failed, %s", err)
	}

	m := make(map[string]int, len(tables1))
	m[task.SrcClusterID+"/"+task.SrcDatabaseName+"/"+baseName] = 1
	for _, k := range tables1 {
		m[k] = m[k] + 1
		if m[k] > services.Cfg.TaskConflictMax {
			return false, common.CodeTaskSrcTableConflict, fmt.Errorf("source table(%v) reached the max num(%v) executing task",
				task.SrcClusterID+"/"+task.SrcDatabaseName, services.Cfg.TaskConflictMax)
		}
	}

	return true, common.CodeOK, nil
}

func GetPolicyActualLastExecDate(ctx *common.Context, policyID uint) (time.Time, error) {
	execDate := time.Time{}
	var task models.Task
	err := ctx.DB.Model(models.Task{}).Where("policy_id =? AND task_status IN (?)", policyID, common.TaskStatusExecTimeImmutable).Last(&task).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return execDate, nil
		}
		return execDate, fmt.Errorf("query models.Task(policy_id =%v AND task_status IN (%v)) from db failed, %s", policyID, common.TaskStatusExecTimeImmutable, err)
	}

	execDate, err = time.ParseInLocation(time.DateOnly, task.ExecuteDate, time.Now().Location())
	if err != nil {
		return execDate, fmt.Errorf("parse task(%v).ExecuteDate(%v) failed, %s", task.ID, task.ExecuteDate, err)
	}

	return execDate, nil
}

func GetExecutingTaskNum(ctx *common.Context) (int, error) {
	var executingTask int64
	err := ctx.DB.Model(models.Task{}).Where("task_status IN (?)", common.TaskStatusExecuting).Count(&executingTask).Error
	if err != nil {
		return 0, fmt.Errorf("query models.Task(task_status IN (%v)) from db failed, %s", common.TaskStatusExecuting, err)
	}
	return int(executingTask), nil
}

func GetExecutingTaskClusterID(ctx *common.Context) ([]string, error) {
	var tasks []models.Task
	err := ctx.DB.Model(models.Task{}).Where("task_status IN (?)", common.TaskStatusExecuting).Find(&tasks).Error
	if err != nil {
		return nil, fmt.Errorf("query models.Task(task_status IN (%v)) from db failed, %s", common.TaskStatusExecuting, err)
	}

	clusters := make([]string, 0, len(tasks))
	for _, task := range tasks {
		clusters = append(clusters, task.SrcClusterID)
	}

	return clusters, nil
}

func GetExecutingTaskDatabases(ctx *common.Context) ([]string, error) {
	var tasks []models.Task
	err := ctx.DB.Model(models.Task{}).Where("task_status IN (?)", common.TaskStatusExecuting).Find(&tasks).Error
	if err != nil {
		return nil, fmt.Errorf("query models.Task(task_status IN (%v)) from db failed, %s", common.TaskStatusExecuting, err)
	}

	databases := make([]string, 0, len(tasks))
	for _, task := range tasks {
		databases = append(databases, task.SrcClusterID+"/"+task.SrcDatabaseName)
	}

	return databases, nil
}

func GetExecutingTaskTables(ctx *common.Context) ([]string, error) {
	var tasks []models.Task
	err := ctx.DB.Model(models.Task{}).Where("task_status IN (?)", common.TaskStatusExecuting).Find(&tasks).Error
	if err != nil {
		return nil, fmt.Errorf("query models.Task(task_status IN (%v)) from db failed, %s", common.TaskStatusExecuting, err)
	}

	tables := make([]string, 0, len(tasks)*2)
	for _, ts := range tasks {
		_, baseName, err := common.CheckSameShardingTables(ts.SrcTablesName)
		if err != nil {
			return nil, fmt.Errorf("check same sharding tables failed, %s", err)
		}
		tables = append(tables, ts.SrcClusterID+"/"+ts.SrcDatabaseName+"/"+baseName)
	}
	return tables, nil
}

// SupplementaryTaskInformation 补充任务信息
// 1，源端校验（磁盘剩余空间、库、表、主键等）
// 2，治理条件校验（where条件）
// 3，目标端校验（连接信息，目标表名）
// 4，表结构一致性校验
// 5，创建任务，更新策略调度时间
func SupplementaryTaskInformation(ctx *common.Context, taskID uint) error {
	log, db := ctx.Log, ctx.DB

	task := &models.Task{}
	err := db.Model(models.Task{}).Where("id =?", taskID).First(&task).Error
	if err != nil {
		err = fmt.Errorf("query models.Task(id=%v) from db failed, %s", taskID, err)
		log.Error(err)
		return err
	}

	// 任务未到执行的前一天
	if !common.JudgeTaskCouldCheckBeforeExec(task.ExecuteDate) {
		err = fmt.Errorf("judge task(%v) could check exec, execution time(%s) not reached", task.Name, task.ExecuteDate)
		log.Error(err)
		return err
	}

	var policy models.Policy
	err = db.Model(models.Policy{}).Where("id =?", task.PolicyID).First(&policy).Error
	if err != nil {
		err = fmt.Errorf("query models.Policy(id=%v) from db failed, %s", task.PolicyID, err)
		log.Error(err)
		return err
	}

	source := &models.Source{}
	err = db.Model(models.Source{}).Where("id =?", policy.SrcID).First(source).Error
	if err != nil {
		err = fmt.Errorf("query models.Source(id=%v) from db failed, %s", policy.SrcID, err)
		log.Error(err)
		return err
	}

	orgTask := *task

	task.TaskStartTime = time.UnixMilli(0)
	task.TaskEndTime = time.UnixMilli(0)
	task.Pause = policy.Pause
	task.TaskStatus = common.TaskStatusWaiting
	task.TaskReason = ""
	task.TaskDetail = ""

	// 源端信息
	task.SrcID = policy.SrcID
	task.SrcName = source.Name
	task.SrcBu = source.Bu
	task.SrcClusterID = source.ClusterID
	task.SrcClusterName = source.ClusterName
	task.SrcDatabaseName = source.DatabaseName
	task.SrcTablesName = source.TablesName
	task.SrcColumns = policy.ArchiveScope

	// 数据清理方式
	task.Govern = policy.Govern
	task.Condition = policy.Condition
	task.RetainSrcData = policy.RetainSrcData
	task.CleaningSpeed = policy.CleaningSpeed

	task.Relevant = policy.Relevant
	task.NotifyPolicy = policy.NotifyPolicy

	defer func() {
		if err = db.Save(task).Error; err != nil {
			log.Errorf("save models.Task(%+v) failed, %s", task, err)
			return
		}
		log.Debugf("save models.Task(%+v)", task)

		if orgTask.TaskReason != task.TaskReason || orgTask.TaskStatus != task.TaskStatus {
			content := ""
			if task.TaskStatus == common.TaskStatusSupplementFailed {
				content = fmt.Sprintf(common.TaskChangeLogSupplementFailed, task.TaskReason, task.TaskDetail)
			} else {
				content = common.TaskChangeLogSupplementSuccess
			}
			services.CreateTaskChangeLog(ctx, task, common.SystemUserName, content)
		}
	}()

	ok, code, err := CheckSource(ctx, source)
	if !ok {
		err = fmt.Errorf("check source(%s) failed, %s", source.Name, err)
		log.Error(err)
		task.TaskStatus = common.TaskStatusSupplementFailed
		task.TaskReason = code.Message
		task.TaskDetail = err.Error()
		return err
	}

	// 校验where条件合法性
	ok, code, err = CheckPolicyCondition(ctx, source, &policy)
	if !ok {
		err = fmt.Errorf("check policy.condition(%s) failed, %s", policy.Condition, err)
		log.Error(err)
		task.TaskStatus = common.TaskStatusSupplementFailed
		task.TaskReason = code.Message
		task.TaskDetail = err.Error()
		return err
	}

	// 只有归档需要校验目标端信息
	if policy.Govern == common.GovernTypeArchive {
		dest := &models.Destination{}
		err = db.Model(models.Destination{}).Where("id =?", policy.DestID).First(dest).Error
		if err != nil {
			err = fmt.Errorf("query models.Destination(id=%v) from db failed, %s", policy.DestID, err)
			log.Error(err)
			task.TaskStatus = common.TaskStatusSupplementFailed
			task.TaskReason = code.Message
			task.TaskDetail = err.Error()
			return err
		}
		ok, code, err = CheckDestination(ctx, dest)
		if !ok {
			err = fmt.Errorf("common Destination(%s) failed, %s", dest.Name, err)
			log.Error(err)
			task.TaskStatus = common.TaskStatusSupplementFailed
			task.TaskReason = code.Message
			task.TaskDetail = err.Error()
			return err
		}

		// 若目标端库名为空，则默认和源库保持一致
		if dest.DatabaseName == "" {
			dest.DatabaseName = source.DatabaseName
		}

		// 则默认和源库保持一致
		dest.TableName, err = common.GenerateDestTableName(source.TablesName, dest.TableName)
		if err != nil {
			err = fmt.Errorf("generate dest table name failed, %s", err)
			log.Error(err)
			task.TaskStatus = common.TaskStatusSupplementFailed
			task.TaskReason = common.CodeTaskGenDestTabNameErr.Message
			task.TaskDetail = err.Error()
			return err
		}

		// TODO 创建目标库和目标表
		// TODO 表结构一致性校验

		task.DestID = policy.DestID
		task.DestName = dest.Name
		task.DestStorage = dest.Storage
		task.DestConnectionID = dest.ConnectionID
		task.DestCompress = dest.Compress
		task.DestDatabaseName = dest.DatabaseName
		task.DestTableName = dest.TableName
	}
	return nil
}

func CheckScheduledTask(ctx *common.Context) {
	log, db := ctx.Log, ctx.DB

	// 提前一天对任务进行调度前检查
	var tasks []models.Task
	err := db.Model(models.Task{}).Where("enable =? AND task_status IN (?)", true, common.TaskStatusScheduled).Find(&tasks).Error
	if err != nil {
		log.Errorf("query models.Task(enable=true AND task_status IN (%v)) from db failed, %s", common.TaskStatusScheduled, err)
		return
	}
	for _, task := range tasks {
		//	提前一天检查任务是否满足执行条件，以便于人可以提前介入修复异常
		// 任务不是一天后执行先不用检查
		if !common.JudgeTaskCouldCheckBeforeExec(task.ExecuteDate) {
			log.Debugf("judge task(%v) could common before exec, execution time(%s) not reached", task.Name, task.ExecuteDate)
			continue
		}
		log.Infof("judge task(%v) could before exec, execution time(%s) has reached", task.Name, task.ExecuteDate)

		handle := &TaskQueueHandle{ID: task.ID, PolicyID: task.ID, HandleID: utils.RandStr(20)}
		log.With(WithExtra[*TaskQueueHandle](handle, ProcQueuing)...).Infof("task %s push queue...", task.Name)

		// 检查并将任务的信息补充完整
		ok := TaskQueue.Push(*handle)
		if ok {
			log.With(WithExtra[*TaskQueueHandle](handle, ProcQueued)...).Infof("task %s push queue sucess", task.Name)
		} else {
			log.With(WithExtra[*TaskQueueHandle](handle, ProcNotQueue)...).Infof("task %s not push queue", task.Name)
		}
	}
}

// AttemptCreateOrUpdateTask 将策略实例化为任务。1，每天定时检查一次；2，在创建策略、修改策略、任务执行完会主动调用
func AttemptCreateOrUpdateTask(ctx *common.Context, policyID uint) {
	log, db := ctx.Log, ctx.DB

	// 分2种情况：
	// 1, 修改策略时。判断条件：该策略下已经存在任务且任务的状态还没到执行中。动作：更新任务执行日期
	// 2, 首次创建策略&任务执行完毕需要重新生成任务时。判断条件：该策略下没有任何任务。创建新的任务，给任务排期

	// 判断该策略需要创建任务还是更新任务
	// 创建任务判断条件：任务已经结束(执行成功,执行失败,执行超时)
	couldCreateTask, err := JudgeCouldCreateTask(ctx, policyID)
	if err != nil {
		log.Errorf("judge need create task by pilicy(%v) failed, %s", policyID, err)
		return
	}
	log.Debugf("policy(%v) need create task flag: %v", policyID, couldCreateTask)

	// 更新任务判断条件：TaskStatusCanUpdate
	couldUpdateTask, err := JudgeCouldUpdateTask(ctx, policyID)
	if err != nil {
		log.Errorf("judge could update task by pilicy(%v) failed, %s", policyID, err)
		return
	}
	log.Debugf("policy(%v) could update task flag: %v", policyID, couldUpdateTask)

	// 既不需要更新任务也不需要创建任务直接退出
	if !couldCreateTask && !couldUpdateTask {
		log.Debugf("the pilicy(%v) neither create nor update task", policyID)
		return
	}

	var policy models.Policy
	err = db.Model(models.Policy{}).Where("id =?", policyID).First(&policy).Error
	if err != nil {
		log.Errorf("query models.Policy(id=%v) from db failed, %s", policyID, err)
		return
	}

	needCreateTask, needUpdateTaskExecDate, needUpdateTaskInfo, needCheckExecuteDate := false, false, false, false
	var task *models.Task
	if couldCreateTask {
		task, err = CreateTask(ctx, &policy)
		if err != nil {
			log.Errorf("create task by policy(%v) failed, %s", policy.ID, err)
			return
		}
		needCreateTask = true
		log.Debugf("policy(%v) will create task: %+v", policyID, task)
	}

	if couldUpdateTask {
		task, err = deleteRedundantTasks(ctx, policyID)
		if err != nil {
			log.Errorf("delete redundant tasks failed, %s", err)
			return
		}

		// 策略改变时会将对应的任务的NeedCheckExecuteDate设置为true，只有NeedCheckExecuteDate==true才需要重新计算任务的执行日期
		needCheckExecuteDate = task.NeedCheckExecuteDate
		if needCheckExecuteDate {
			needUpdateTaskExecDate, err = calculateTaskExecDate(ctx, &policy, task)
			if err != nil {
				log.Errorf("calculate task(%v) exec date failed, %s", task.ID, err)
				return
			}

			if needUpdateTaskExecDate {
				log.Infof("task(%v) need update exec date(%v)", task.ID, task.ExecuteDate)
			} else {
				log.Debugf("task(%v) do not need update exec date(%v)", task.ID, task.ExecuteDate)
			}

			// 策略其他字段被修改(condition,clean_src,cleaning_speed等)需要更新任务
			needUpdateTaskInfo, err = calculateNeedUpdateTaskInfo(ctx, &policy, task)
			if err != nil {
				log.Errorf("calculate need update task(%v) info failed, %s", task.ID, err)
				return
			}
			if needUpdateTaskInfo {
				log.Infof("task(%v) need update info", task.ID)
			} else {
				log.Debugf("task(%v) do not need update info", task.ID)
			}

			task.NeedCheckExecuteDate = false
		}
	}

	// 任务执行日期未发生变化无操作需要立刻进行执行前检查
	if !needCreateTask && !needUpdateTaskExecDate && !needUpdateTaskInfo && !needCheckExecuteDate {
		log.Debugf("tasks(%v) do not need update", task.ID)
		return
	}

	if err = db.Save(task).Error; err != nil {
		log.Errorf("save models.Task(%+v) failed, %s", task, err)
	}

	if err != nil {
		if needCreateTask {
			log.With(ProcScheduleFailed).Errorf("create task(%v) faield, %s", task.Name, err)
		}
		if needUpdateTaskExecDate || needUpdateTaskInfo {
			log.With(ProcScheduleUpdateFailed).Errorf("update task(%v) faield, %s", task.Name, err)
		}
		log.Infof("task detail: %+v", task)
		return
	} else {
		if needCreateTask {
			services.CreateTaskChangeLog(ctx, task, common.SystemUserName, common.TaskChangeLogCreate)
			log.With(ProcScheduled).Infof("create task(%v) success", task.Name)
			log.With(ProcScheduleUpdated).Infof("create and update task(%v) success", task.Name)
		}
		if needUpdateTaskExecDate || needUpdateTaskInfo {
			services.CreateTaskChangeLog(ctx, task, common.SystemUserName,
				fmt.Sprintf(common.TaskChangeLogUpdateByPolicy, task.ExecuteDate, string(task.ExecuteWindow)))
			log.With(ProcScheduleUpdated).Infof("update task(%v) success", task.Name)
		}
		log.Infof("task detail: %+v", task)
	}

	// 任务执行日期发生变化需要立刻进行执行前检查

	// 创建的任务需要从库里重新拿到ID
	if needCreateTask {
		err = db.Model(models.Task{}).Where("name=?", task.Name).Last(&task).Error
		if err != nil {
			log.Errorf("query models.Task(name=%s) from db failed, %s", task.Name, err)
			return
		}
	}

	if !common.JudgeTaskCouldCheckBeforeExec(task.ExecuteDate) {
		err = fmt.Errorf("judge task(%v) could exec check, execution time(%s) not reached", task.Name, task.ExecuteDate)
		log.Info(err)
		return
	}

	log.With(ProcQueuing).Infof("will scheduled task check task(%v)", task.ID)
	handleID, ok := ctx.GetData(HandleID)
	if !ok {
		handleID = utils.RandStr(20)
	}
	handle := &TaskQueueHandle{ID: task.ID, PolicyID: task.PolicyID, HandleID: handleID.(string)}
	ok = TaskQueue.Push(*handle)
	if ok {
		log.With(ProcQueued).Infof("task %s push queue sucess", task.Name)
	} else {
		log.With(ProcNotQueue).Infof("task %s not push queue", task.Name)
	}
}

func deleteRedundantTasks(ctx *common.Context, policyID uint) (*models.Task, error) {
	log, db := ctx.Log, ctx.DB

	var tasks []models.Task
	err := db.Model(models.Task{}).Where("policy_id =? AND task_status IN (?)", policyID, common.TaskStatusCanUpdate).Find(&tasks).Order("id desc").Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("query models.Task(policy_id=%v AND task_status IN (%v)) from db failed, %s", policyID, common.TaskStatusCanUpdate, err)
	}

	if len(tasks) < 1 {
		return nil, fmt.Errorf("query models.Task(policy_id=%v AND task_status IN (%v)) from db failed, %s", policyID, common.TaskStatusCanUpdate, err)
	}

	// 同一个策略理论上只能有一个正在运行的任务，如果有多个需要删除只留一个
	for i := 1; i < len(tasks); i++ {
		//  删除多余的任务
		err = db.Delete(models.Task{}, "id =?", tasks[i].ID).Error
		if err != nil {
			log.Errorf("delete redundant task(id=%v) from db failed, %s", tasks[i].ID, err)
		}
		log.Warnf("has delete redundant tasks(%v) with policy(%v)", tasks[i].ID, policyID)
	}
	return &tasks[0], err
}

func setPeriodOnceTaskDisable(ctx *common.Context, task *models.Task) error {
	var policy models.Policy
	err := ctx.DB.Model(models.Policy{}).Where("id =?", task.PolicyID).First(&policy).Error
	if err != nil {
		return fmt.Errorf("query models.Policy(id=%v) from db failed, %s", task.PolicyID, err)
	}

	if policy.Period != common.PeriodOnce {
		return nil
	}

	if policy.Enable == false {
		return nil
	}

	if err = ctx.DB.Model(models.Policy{}).Where("id=?", policy.ID).Update("enable", false).Error; err != nil {
		return fmt.Errorf("update models.Policy(%+v) enable=false failed, %s", policy.ID, err)
	}
	return nil
}

// CheckDiskUsage 定时同步任务和workflow的状态
func CheckDiskUsage(ctx *common.Context) {
	log, db := ctx.Log, ctx.DB

	if !services.Cfg.SourceStatusDetect {
		log.Infof("source status detect is disabled")
		return
	}
	var tasks []models.Task
	err := db.Model(models.Task{}).Where("task_status =?", common.TaskStatusExecuting).Find(&tasks).Error
	if err != nil {
		log.Errorf("query models.Task(task_status=%v) from db failed, %s", common.TaskStatusExecuting, err)
		return
	}

	ginCtx := middlewares.NewGinContext(ctx.Log, ctx.DB)
	for _, task := range tasks {
		clusterSvc, err := services.GetClusterServiceByClusterID(ginCtx, task.SrcClusterID)
		if err != nil {
			log.Errorf("get cluster(%s) failed, %s", task.SrcClusterID, err)
			continue
		}

		diskUsage, err := services.NewClusterDriver(clusterSvc).GetDiskUsage(ginCtx)
		if err != nil {
			log.Errorf("get cluster(%s) disk usage failed, %s", clusterSvc.ClusterID, err)
			continue
		}

		if diskUsage > services.Cfg.SourceStatusDetectDiskUsage {
			err = workflow1.NewDriver(configs.C.WorkFlow.Driver).StopWorkFlow(ctx, task.WorkFlow)
			msg := fmt.Sprintf("cluster(%s) current disk usage(%d) > threshold(%d)", clusterSvc.ClusterID, diskUsage, services.Cfg.SourceStatusDetectDiskUsage)
			log.Info(msg)
			if err != nil {
				log.Errorf("stop task(%v) workflow(%s) failed, %s", task.ID, task.WorkFlow, err)
			} else {
				log.Infof("stop task(%v) workflow(%s) success", task.ID, task.WorkFlow)
				services.CreateTaskChangeLog(ctx, &task, common.SystemUserName, fmt.Sprintf(common.TaskChangeLogStopWorkFlow, msg))
			}
		}
	}
}
