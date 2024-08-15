package models

type PolicyRevision struct {
	Model

	PolicyID    uint   `json:"policy_id" gorm:"type:int;index:policy_id_idx;comment:策略ID"`
	ModifyField string `json:"modify_field" gorm:"type:varchar(128);comment:修改字段"`
	OldValue    string `json:"old_value" gorm:"type:longtext;comment:原始值"`
	NewValue    string `json:"new_value" gorm:"type:longtext;comment:修改值"`
}

type TaskRevision struct {
	Model
	TaskID      uint   `json:"task_id" gorm:"type:int;index:task_id_idx;comment:任务ID"`
	ModifyField string `json:"modify_field" gorm:"type:varchar(128);comment:修改字段"`
	OldValue    string `json:"old_value" gorm:"type:longtext;comment:原始值"`
	NewValue    string `json:"new_value" gorm:"type:longtext;comment:修改值"`
}
