package models

import "github.com/sunkaimr/data-loom/internal/pkg/common"

type Connection struct {
	Model

	// 连接信息
	Name         string             `json:"name" gorm:"type:varchar(1024);not null;comment:连接名称"`
	Description  string             `json:"description" gorm:"type:longtext;default '';comment:说明"`
	Bu           string             `json:"bu" gorm:"type:varchar(64);comment:资产BU"`
	Storage      common.StorageType `json:"storage" gorm:"type:varchar(64);comment:归档介质"`
	DataBendAK   string             `json:"data_bend_ak" gorm:"type:varchar(1024);comment:DataBend的AK"`
	DataBendSK   string             `json:"data_bend_sk" gorm:"type:varchar(1024);comment:DataBend的SK"`
	DataBendAddr string             `json:"data_bend_addr" gorm:"type:varchar(1024);comment:DataBend地址"`
	MysqlHost    string             `json:"mysql_host" gorm:"type:varchar(1024);comment:mysql地址"`
	MysqlPort    string             `json:"mysql_port" gorm:"type:varchar(1024);comment:mysql端口"`
	MysqlUser    string             `json:"mysql_user" gorm:"type:varchar(1024);comment:mysql用户名"`
	MysqlPasswd  string             `json:"mysql_passwd" gorm:"type:varchar(1024);comment:mysql密码"`
}
