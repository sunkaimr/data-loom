package models

import (
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"time"
)

// TaskChangeLog 任务状态变化记录
type TaskChangeLog struct {
	ID         uint                  `json:"id" gorm:"primary_key;AUTO_INCREMENT;comment:ID"`
	TaskID     uint                  `json:"task_id" gorm:"type:int;index:task_id_idx;comment:任务ID"`
	Time       time.Time             `json:"time" gorm:"type:DATETIME;comment:创建时间"`
	UserName   string                `json:"user_name" gorm:"type:varchar(64);comment:创建人"`
	TaskStatus common.TaskStatusType `json:"task_status" gorm:"type:varchar(64);comment:任务状态"`
	Content    string                `json:"content" gorm:"type:longtext;comment:变化内容记录"`
}
