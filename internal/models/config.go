package models

import (
	. "github.com/sunkaimr/data-loom/internal/pkg/common"
)

type Config struct {
	ID uint `json:"id" gorm:"primary_key"`
	// 集群相关配置
	ClusterType            string `json:"cluster_type" gorm:"type:varchar(255);comment:集群类型"`
	ClusterDefaultUser     string `json:"cluster_default_user" gorm:"type:varchar(1024);comment:源端用户名"`
	ClusterDefaultPasswd   string `json:"cluster_default_passwd" gorm:"type:varchar(1024);comment:源端密码"`
	ClusterExcludeDatabase string `json:"src_exclude_database" gorm:"type:varchar(1024);comment:源端库黑名单"`
	ClusterExcludeTables   string `json:"src_exclude_tables" gorm:"type:varchar(1024);comment:源端表黑名单"`

	// 任务相关配置
	TaskMaxParallel       int                   `json:"task_max_parallel" gorm:"type:int(4);comment:任务的最大并法数"`
	TaskTimeout           int                   `json:"task_timeout" gorm:"type:int(4);comment:任务的超时时间,单位秒"`
	TaskConflictLevel     TaskConflictLevelType `json:"task_conflict_level" gorm:"type:varchar(20);comment:任务冲突级别：源端集群级别，库级别，表级别"`
	TaskConflictMax       int                   `json:"task_conflict_max" gorm:"type:int(4);comment:每个级别最多允许运行几个任务"`
	WorkflowRetentionDays int                   `json:"workflow_retention_days" gorm:"type:int(4);comment:任务的工作流保留天数"`

	ThanosUrl string `json:"thanos_url" gorm:"type:varchar(1024);comment:监控指标地址"`

	SourceStatusDetect          bool `json:"source_status_detect" gorm:"type:int(4);comment:开启源端库运行状态检测"`
	SourceStatusDetectDiskUsage int  `json:"source_status_detect_disk_usage" gorm:"type:int(4);comment:源端磁盘使用率"`

	Notice                  NoticeType `json:"notice" gorm:"type:varchar(255);comment:通知方式"`
	EmailHost               string     `json:"email_host" gorm:"type:varchar(1024);comment:邮件服务器地址"`
	EmailPort               int        `json:"email_port" gorm:"type:int(4);comment:邮件服务器端口"`
	EmailUsername           string     `json:"email_username" gorm:"type:varchar(1024);comment:用户名"`
	EmailPassword           string     `json:"email_password" gorm:"type:varchar(1024);comment:密码"`
	EmailInsecureSkipVerify bool       `json:"email_insecure_skip_verify" gorm:"type:int(4);comment:不检验服务端证书"`
}
