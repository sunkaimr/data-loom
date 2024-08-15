package status

import (
	. "github.com/sunkaimr/data-loom/internal/pkg/queue"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"go.uber.org/zap"
	"strconv"
)

const (
	PolicyID        = "policyID"
	TaskID          = "taskID"
	HandleID        = "handleID"
	QueueType       = "queueType"
	QueueTypePolicy = "policy"
	QueueTypeTask   = "task"
)

// 入队列 > 出队列 > 创建任务计划 > 生成任务详情 > 执行前检查 > 执行中 > 完成
var (
	// 入队列
	ProcQueuing  = zap.String("process", "queuing")    // 准备入队列
	ProcNotQueue = zap.String("process", "not_queued") // 未入队列
	ProcQueued   = zap.String("process", "queued")     // 入队列
	// 出队列
	ProcDequeue = zap.String("process", "dequeue") // 出队列
	// 创建任务计划
	ProcScheduled            = zap.String("process", "scheduled")               // 任务创建成功
	ProcScheduleFailed       = zap.String("process", "scheduled_failed")        // 任务创建失败
	ProcScheduleUpdated      = zap.String("process", "scheduled_updated")       // 执行日期已更新
	ProcScheduleUpdateFailed = zap.String("process", "scheduled_update_failed") // 执行日期更新失败
	// 补充任务详细信息
	ProcSupplemented     = zap.String("process", "supplemented")      // 补充任务详细信息成功
	ProcSupplementFailed = zap.String("process", "supplement_failed") // 补充任务详细信息失败
	// 执行前检查
	ProcExecCheckOk     = zap.String("process", "exec_check")        // 执行前检查通过
	ProcExecCheckFailed = zap.String("process", "exec_check_failed") // 执行前检查通过	// 执行中
	// 执行中
	ProcCallWorkflowFailed = zap.String("process", "call_workflow_failed") // 执行中
	ProcExecuting          = zap.String("process", "executing")            // 执行中
	// 完成
	ProcExecSuccess = zap.String("process", "success") // 执行成功
	ProcExecFailed  = zap.String("process", "failed")  // 执行失败
	ProcExecTimeout = zap.String("process", "timeout") // 执行超时
)

func WithExtra[T *PolicyQueueHandle | *TaskQueueHandle](h T, extra ...any) []any {
	s := make([]any, 0, 3+len(extra))
	switch v := any(h).(type) {
	case *PolicyQueueHandle:
		policyIDStr := utils.Ternary[string](v.ID == 0, "", strconv.Itoa(int(v.ID)))
		s = append(s, zap.String(QueueType, QueueTypePolicy))
		s = append(s, zap.String(PolicyID, policyIDStr))
		s = append(s, zap.String(TaskID, ""))
		s = append(s, zap.String(HandleID, v.HandleID))
	case *TaskQueueHandle:
		taskIDStr := utils.Ternary[string](v.ID == 0, "", strconv.Itoa(int(v.ID)))
		policyIDStr := utils.Ternary[string](v.PolicyID == 0, "", strconv.Itoa(int(v.PolicyID)))
		s = append(s, zap.String(QueueType, QueueTypeTask))
		s = append(s, zap.String(PolicyID, policyIDStr))
		s = append(s, zap.String(TaskID, taskIDStr))
		s = append(s, zap.String(HandleID, v.HandleID))
	}
	for _, v := range extra {
		s = append(s, v)
	}
	return s
}
