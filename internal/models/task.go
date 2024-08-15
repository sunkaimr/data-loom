package models

import (
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"time"
)

type Task struct {
	Model

	// 归档任务元数据信息
	Name          string `json:"name" gorm:"type:varchar(1024);not null;comment:任务名称"`
	Description   string `json:"description" gorm:"type:longtext;default '';comment:说明"`
	Enable        bool   `json:"enable" gorm:"type:int(4);comment:是否生效"`
	PolicyID      uint   `json:"policy_id" gorm:"type:int;index:policy_id_idx;comment:策略ID"`
	ExecuteWindow JSON   `json:"execute_window" gorm:"type:json;comment:执行窗口"`
	ExecuteDate   string `json:"execute_date" gorm:"varchar(20);index:execute_date_idx;comment:计划执行日期"`
	Pause         bool   `json:"pause" gorm:"type:int(4);comment:执行窗口外是否需要暂停执行"`
	RebuildFlag   bool   `json:"rebuild_flag" gorm:"type:int(4);comment:执行窗口外是否重建表(仅在治理方式是删除时有效)"`

	// 源端信息
	SrcID                  uint   `json:"src_id" gorm:"type:int;index:src_id;comment:任务ID"`
	SrcName                string `json:"src_name" gorm:"type:varchar(1024);not null;comment:源端名称"`
	SrcBu                  string `json:"src_bu" gorm:"type:varchar(64);comment:资产BU"`
	SrcClusterName         string `json:"src_cluster_name" gorm:"type:varchar(128);not null;index:src_cluster_name_idx;comment:集群名称"`
	SrcClusterID           string `json:"src_cluster_id" gorm:"type:varchar(128);not null;index:src_cluster_id_idx;comment:集群ID"`
	SrcDatabaseName        string `json:"src_database_name" gorm:"type:varchar(128);comment:源库名"`
	SrcTablesName          string `json:"src_tables_name" gorm:"type:longtext;comment:源表名"`
	SrcColumns             string `json:"src_columns" gorm:"type:longtext;comment:列名"`
	SrcClusterFreeDisk     int    `json:"src_cluster_free_disk" gorm:"type:int;comment:磁盘剩余空间"`
	SrcClusterSumTableSize int    `json:"src_cluster_sum_table_size" gorm:"type:int;comment:清理前表大小"`

	// 目标端信息
	DestID           uint               `json:"dest_id" gorm:"type:int;index:dest_id;comment:目标端ID"`
	DestName         string             `json:"dest_name" gorm:"type:varchar(1024);not null;comment:目标端名称"`
	DestStorage      common.StorageType `json:"dest_storage" gorm:"type:varchar(64);comment:归档介质"`
	DestConnectionID uint               `json:"dest_connection_id" gorm:"type:int;comment:归档库连接信息"`
	DestDatabaseName string             `json:"dest_database_name" gorm:"type:varchar(128);comment:归档库名"`
	DestTableName    string             `json:"dest_table_name" gorm:"type:varchar(128);comment:归档表名"`
	DestCompress     bool               `json:"dest_compress" gorm:"type:int(4);comment:是否压缩存储"`

	// 数据治理方式
	Govern        common.GovernType        `json:"govern" gorm:"type:varchar(64);comment:数据治理方式"`
	Condition     string                   `json:"condition" gorm:"type:longtext;comment:数据治理条件"`
	RetainSrcData bool                     `json:"retain_src_data" gorm:"type:int(4);comment:归档时否保留源表数据"`
	CleaningSpeed common.CleaningSpeedType `json:"cleaning_speed" gorm:"type:varchar(64);comment:清理速度"`

	// 通知策略
	Relevant     JSON                    `json:"relevant" gorm:"type:json;comment:关注人"`
	NotifyPolicy common.NotifyPolicyType `json:"notify_policy" gorm:"type:varchar(64);not null;comment:通知策略"`

	// 结果
	TaskResultQuantity   int                   `json:"task_result_quantity" gorm:"type:int;comment:治理数据量"`
	TaskResultSize       int                   `json:"task_result_size" gorm:"type:int;comment:治理数据容量(MB)"`
	TaskStartTime        time.Time             `json:"task_start_time" gorm:"type:DATETIME;index:task_start_time_idx;comment:开始执行时间"`
	TaskEndTime          time.Time             `json:"task_end_time" gorm:"type:DATETIME;index:task_end_time_idx;comment:执行结束时间"`
	TaskDuration         int                   `json:"task_duration" gorm:"type:int;comment:执行时长(秒)"`
	WorkFlow             string                `json:"workflow" gorm:"type:varchar(1024);comment:工作流"`
	TaskStatus           common.TaskStatusType `json:"task_status" gorm:"type:varchar(64);index:task_status_idx;comment:任务状态"`
	TaskReason           string                `json:"task_reason" gorm:"type:longtext;comment:任务失败原因"`
	TaskDetail           string                `json:"task_detail" gorm:"type:longtext;comment:任务失败详情"`
	NeedCheckExecuteDate bool                  `json:"need_check_execute_date" gorm:"type:int(4);default:0;comment:是否需要更新执行日期"`
}
