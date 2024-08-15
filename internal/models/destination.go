package models

import "github.com/sunkaimr/data-loom/internal/pkg/common"

type Destination struct {
	Model

	// 目标端信息
	Name         string             `json:"name" gorm:"type:varchar(1024);not null;comment:目标端名称"`
	Description  string             `json:"description" gorm:"type:longtext;default '';comment:说明"`
	Storage      common.StorageType `json:"storage" gorm:"type:varchar(64);comment:归档介质"`
	ConnectionID uint               `json:"connection_id" gorm:"type:int;comment:归档库连接信息"`
	DatabaseName string             `json:"database_name" gorm:"type:varchar(128);comment:归档库名"`
	TableName    string             `json:"table_name" gorm:"type:varchar(128);comment:归档表名"`
	Compress     bool               `json:"compress" gorm:"type:int(4);comment:是否压缩存储"`
}
