package common

import "math"

const InvalidUint uint = math.MaxUint
const InvalidInt int = math.MaxInt

const (
	RequestID = "RequestID"
	JWT       = "JWT"
	LOGGER    = "LOGGER"
	DB        = "DB"

	AdminUser                = "admin"
	AdminName                = "管理员"
	AdminPasswd              = "123456"
	SystemUser               = "system"
	SystemUserName           = "系统"
	UpdateTaskResultUser     = "updater"
	UpdateTaskResultUserName = "工作流"
)

type ClusterImportFromType string

const (
	ImportFromCustomized ClusterImportFromType = "customized"
)

type ClusterType string

const (
	ClusterTypeMysql ClusterType = "mysql"
	ClusterTypeOther ClusterType = ""
)

type NoticeType string

const (
	NoticeTypeEmail NoticeType = "email"
)

// StorageType 归档存贮介质
// 需提供的接口：
// - 检查连接状态
// - 判断库是否存在
// - 判断表是否存在
// - 创建库
// - 创建表
// - 判断表结构是否一致
// - 更新表结构
type StorageType string

const (
	StorageMysql    StorageType = "mysql"
	StorageDataBend StorageType = "databend"
)

// PeriodType 执行周期
type PeriodType string

const (
	PeriodOnce      PeriodType = "once"       // 执行一次
	PeriodDay       PeriodType = "day"        // 每天
	PeriodTwoDay    PeriodType = "two-day"    // 每二天一次
	PeriodWeekly    PeriodType = "weekly"     // 每周一次
	PeriodTwoWeeks  PeriodType = "two-week"   // 两周一次
	PeriodMonthly   PeriodType = "monthly"    // 每月一次
	PeriodQuarterly PeriodType = "quarterly"  // 每季度一次
	PeriodSixMonths PeriodType = "six-months" // 每半年一次
	PeriodYearly    PeriodType = "yearly"     // 每年一次
)

var PeriodCN = map[PeriodType]string{
	PeriodOnce:      "单次任务",
	PeriodDay:       "每天任务",
	PeriodTwoDay:    "双天任务",
	PeriodWeekly:    "周度任务",
	PeriodTwoWeeks:  "双周任务",
	PeriodMonthly:   "月度任务",
	PeriodQuarterly: "季度任务",
	PeriodSixMonths: "半年度任务",
	PeriodYearly:    "年度任务",
}

// CleaningSpeedType 清理速速
type CleaningSpeedType string

const (
	CleaningSpeedSteady   CleaningSpeedType = "steady"   //稳定优先
	CleaningSpeedBalanced CleaningSpeedType = "balanced" //速度适中
	CleaningSpeed         CleaningSpeedType = "swift"    //速度优先
	CleaningSpeedDefault                    = CleaningSpeedBalanced
)

// GovernType 清理类型
type GovernType string

const (
	GovernTypeTruncate     GovernType = "truncate"      // 清空数据
	GovernTypeDelete       GovernType = "delete"        // 删除数据
	GovernTypeBackupDelete GovernType = "backup-delete" // 备份后删除
	GovernTypeArchive      GovernType = "archive"       // 归档数据
	GovernTypeRebuild      GovernType = "rebuild"       // 重建表
)

var GovernCN = map[GovernType]string{
	GovernTypeTruncate:     "清空数据",
	GovernTypeDelete:       "删除数据",
	GovernTypeBackupDelete: "备份后删除",
	GovernTypeArchive:      "归档数据",
	GovernTypeRebuild:      "重建表",
}

// NotifyPolicyType 通知策略
type NotifyPolicyType string

const (
	NotifyPolicyTypeSilence NotifyPolicyType = "silence" // 不通知
	NotifyPolicyTypeSuccess NotifyPolicyType = "success" // 成功时通知
	NotifyPolicyTypeFailed  NotifyPolicyType = "failed"  // 失败时通知
	NotifyPolicyTypeAlways  NotifyPolicyType = "always"  // 成功或失败都通知

	NotifyPolicyTypeDefault = NotifyPolicyTypeAlways
)

type TaskConflictLevelType string

const (
	TaskConflictLevelCluster  TaskConflictLevelType = "cluster"  // 集群级
	TaskConflictLevelDatabase TaskConflictLevelType = "database" // 库级
	TaskConflictLevelTable    TaskConflictLevelType = "table"    // 表级
)

// TaskStatusType 任务状态
type TaskStatusType string

const (
	TaskStatusScheduled        TaskStatusType = "scheduled"         // 已排期
	TaskStatusSupplementFailed TaskStatusType = "supplement_failed" // 填充任务信息失败
	TaskStatusWaiting          TaskStatusType = "waiting"           // 等待执行
	TaskStatusExecCheckFailed  TaskStatusType = "exec_check_failed" // 执行前检查失败
	TaskStatusExecuting        TaskStatusType = "executing"         // 执行中
	TaskStatusSuccess          TaskStatusType = "success"           // 执行成功
	TaskStatusExecFailed       TaskStatusType = "failed"            // 执行失败
	TaskStatusTimeout          TaskStatusType = "timeout"           // 执行超时
)

var TaskStatusCN = map[TaskStatusType]string{
	TaskStatusScheduled:        "已排期",
	TaskStatusSupplementFailed: "填充任务信息失败",
	TaskStatusWaiting:          "等待执行",
	TaskStatusExecCheckFailed:  "执行前检查失败",
	TaskStatusExecuting:        "执行中",
	TaskStatusSuccess:          "执行成功",
	TaskStatusExecFailed:       "执行失败",
	TaskStatusTimeout:          "执行超时",
}

// TaskStatusHasFinished 任务已经结束(执行成功,执行失败,执行超时)
var TaskStatusHasFinished = []TaskStatusType{TaskStatusSuccess, TaskStatusExecFailed, TaskStatusTimeout}

// TaskStatusExecTimeImmutable 任务已经开始或结束, 任务的执行时间不会再更改了
var TaskStatusExecTimeImmutable = []TaskStatusType{TaskStatusExecuting, TaskStatusSuccess, TaskStatusExecFailed, TaskStatusTimeout}

// TaskStatusCanExec 可以调用工作流的任务
var TaskStatusCanExec = []TaskStatusType{TaskStatusWaiting, TaskStatusExecCheckFailed}

// TaskStatusCanUpdate 以下状态的任务还没有进入执行状态可以更改
var TaskStatusCanUpdate = []TaskStatusType{TaskStatusScheduled, TaskStatusSupplementFailed, TaskStatusWaiting, TaskStatusExecCheckFailed}

// TaskStatusNotFinish 任务未完成的不可删除
var TaskStatusNotFinish = []TaskStatusType{TaskStatusScheduled, TaskStatusSupplementFailed, TaskStatusWaiting, TaskStatusExecCheckFailed, TaskStatusExecuting}

const (
	TaskChangeLogCreate              = "创建任务"
	TaskChangeLogUpdateByPolicy      = "策略发生变动，同步更新任务，执行日期：%s 执行窗口：%s"
	TaskChangeLogUpdate              = "任务发生变动，执行日期：%s 执行窗口：%s"
	TaskChangeLogSupplementSuccess   = "补充任务信息"
	TaskChangeLogSupplementFailed    = "补充任务信息失败，原因：%s，详情：%s"
	TaskChangeLogWaitingExec         = "任务等待执行，原因：%s，详情：%s"
	TaskChangeLogCallWorkFlowSuccess = "调用工作流"
	TaskChangeLogCallWorkFlowFailed  = "调用工作流失败，原因：%s，详情：%s"
	TaskChangeLogStopWorkFlow        = "停止工作流，原因：源端磁盘空间不足，详情：%s"
	TaskChangeLogWorkFlowFinished    = "工作流结束"
)
