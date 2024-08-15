package models

import (
	"github.com/sunkaimr/data-loom/internal/pkg/common"
)

type Policy struct {
	Model

	// 归档策略元数据信息
	Name          string                   `json:"name" gorm:"type:varchar(1024);not null;comment:策略名称"`
	Description   string                   `json:"description" gorm:"type:longtext;default '';comment:说明"`
	Bu            string                   `json:"bu" gorm:"type:varchar(64);comment:资产BU"`
	Enable        bool                     `json:"enable" gorm:"type:int(4);comment:是否生效"`
	Period        common.PeriodType        `json:"period" gorm:"type:varchar(64);not null;comment:执行周期"`
	Day           int                      `json:"day" gorm:"type:int(4);comment:期望执行日期"`
	ExecuteWindow JSON                     `json:"execute_window" gorm:"type:json;comment:执行窗口"`
	Pause         bool                     `json:"pause" gorm:"type:int(4);comment:执行窗口外是否需要暂停执行"`
	RebuildFlag   bool                     `json:"rebuild_flag" gorm:"type:int(4);comment:执行窗口外是否重建表(仅在治理方式是删除时有效)"`
	CleaningSpeed common.CleaningSpeedType `json:"cleaning_speed" gorm:"type:varchar(64);comment:清理速度"`

	// 源端信息
	SrcID uint `json:"src_id" gorm:"type:int;index:src_idx;comment:源端ID"`

	// 数据治理方式
	Govern        common.GovernType `json:"govern" gorm:"type:varchar(64);comment:数据治理方式"`
	Condition     string            `json:"condition" gorm:"type:longtext;comment:数据治理条件"`
	ArchiveScope  string            `json:"archive_scope" gorm:"type:longtext;comment:数据归档范围，归档那些列（仅归档涉及）"`
	RetainSrcData bool              `json:"retain_src_data" gorm:"type:int(4);comment:归档时否保留源表数据"`

	// 目标端信息
	DestID uint `json:"dest_id" gorm:"type:int;index:dest_idx;comment:目标端ID"`

	// 结果通知
	Relevant     JSON                    `json:"relevant" gorm:"type:json;comment:关注人"`
	NotifyPolicy common.NotifyPolicyType `json:"notify_policy" gorm:"type:varchar(64);not null;comment:通知策略"`
}
