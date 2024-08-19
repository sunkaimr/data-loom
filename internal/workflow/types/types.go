package types

import (
	"github.com/sunkaimr/data-loom/internal/pkg/common"
)

// WorkFlowStatusType 工作流状态
type WorkFlowStatusType string

const (
	WorkFlowStatusUnknown   WorkFlowStatusType = ""          // 未知状态
	WorkFlowStatusPending   WorkFlowStatusType = "Pending"   // 未知状态
	WorkFlowStatusRunning   WorkFlowStatusType = "Running"   // 正在运行
	WorkFlowStatusSucceeded WorkFlowStatusType = "Succeeded" // 成功
	WorkFlowStatusFailed    WorkFlowStatusType = "Failed"    // 成功
	WorkFlowStatusError     WorkFlowStatusType = "Error"     // 失败
)

// TaskResultService 任务的执行结果
type TaskResultService struct {
	ID                 uint                  `json:"id"`                   // 任务ID
	TaskStatus         common.TaskStatusType `json:"task_status"`          // 任务状态
	TaskReason         string                `json:"task_reason"`          // 任务失败原因
	TaskDetail         string                `json:"task_detail"`          // 任务失败详情
	TaskResultQuantity int                   `json:"task_result_quantity"` // 治理数据量
	TaskResultSize     int                   `json:"task_result_size"`     // 治理数据容量
	TaskStartTime      string                `json:"task_start_time"`      // 开始执行时间 "2006-01-02 15:04:05"
	TaskEndTime        string                `json:"task_end_time"`        // 执行结束时间 "2006-01-02 15:04:05"
}

type TruncateParaStruct struct {
	TaskID         uint              `json:"task_id"`
	Host           string            `json:"host"`
	Port           string            `json:"port"`
	User           string            `json:"user"`
	Password       string            `json:"password"`
	Database       string            `json:"database"`
	Tables         string            `json:"tables"`
	Callback       Callback          `json:"callback"`
	CallbackResult TaskResultService `json:"callback_result"`
}

type DeleteParaStruct struct {
	TaskID         uint                     `json:"task_id"`
	StartTime      string                   `json:"start_time"`
	EndTime        string                   `json:"end_time"`
	Host           string                   `json:"host"`
	Port           string                   `json:"port"`
	User           string                   `json:"user"`
	Password       string                   `json:"password"`
	Database       string                   `json:"database"`
	Tables         string                   `json:"tables"`
	Condition      string                   `json:"condition"`
	FreeDisk       string                   `json:"free_disk"`
	RebuildFlag    bool                     `json:"rebuild_flag"`
	CleaningSpeed  common.CleaningSpeedType `json:"cleaning_speed"`
	Callback       Callback                 `json:"callback"`
	CallbackResult TaskResultService        `json:"callback_result"`
}

type ArchiveParaStruct struct {
	TaskID         uint                     `json:"task_id"`
	Host           string                   `json:"host"`
	Port           string                   `json:"port"`
	User           string                   `json:"user"`
	Password       string                   `json:"password"`
	Database       string                   `json:"database"`
	Tables         string                   `json:"tables"`
	Condition      string                   `json:"condition"`
	CleaningSpeed  common.CleaningSpeedType `json:"cleaning_speed"`
	FreeDisk       string                   `json:"free_disk"`
	Callback       Callback                 `json:"callback"`
	CallbackResult TaskResultService        `json:"callback_result"`
}

type RebuildParaStruct struct {
	TaskID         uint              `json:"task_id"`
	Host           string            `json:"host"`
	Port           string            `json:"port"`
	User           string            `json:"user"`
	Password       string            `json:"password"`
	Database       string            `json:"database"`
	Tables         string            `json:"tables"`
	FreeDisk       string            `json:"free_disk"`
	Callback       Callback          `json:"callback"`
	CallbackResult TaskResultService `json:"callback_result"`
}

type Callback struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

type Extra struct {
	FreeDisk int `json:"free_disk"`
}
