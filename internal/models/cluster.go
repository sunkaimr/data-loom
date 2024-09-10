package models

import "github.com/sunkaimr/data-loom/internal/pkg/common"

type Cluster struct {
	Model
	ClusterName string                       `json:"cluster_name" gorm:"type:varchar(64);not null;comment:集群名称"`
	ClusterID   string                       `json:"cluster_id" gorm:"type:varchar(64);index:idx_cluster_id;comment:集群ID"`
	Description string                       `json:"description" gorm:"type:longtext;comment:说明"`
	Bu          string                       `json:"bu" gorm:"type:varchar(64);comment:bu"`
	Env         string                       `json:"env" gorm:"type:varchar(64);comment:环境"`
	ImportFrom  common.ClusterImportFromType `json:"import_from" gorm:"type:varchar(64);comment:添加方式：自定义添加、从资源中心或其他地方导入"`
	ClusterType common.ClusterType           `json:"cluster_type" gorm:"type:varchar(64);comment:集群类型（mysql, 其他的）"`
	ServiceAddr string                       `json:"service_addr" gorm:"type:varchar(1024);comment:服务地址(vip) ip:port"`
	WriteAddr   string                       `json:"write_addr" gorm:"type:varchar(1024);comment:写库地址 ip:port"`
	ReadAddr    string                       `json:"read_addr" gorm:"type:varchar(1024);comment:读库地址 ip:port"`
	UserName    string                       `json:"user_name" gorm:"type:varchar(1024);comment:用户名"`
	Password    string                       `json:"password" gorm:"type:varchar(1024);comment:密码"`
}

// ClusterStatistics 集群的表大小统计信息
type ClusterStatistics struct {
	Model
	Date        string `json:"date" gorm:"type:varchar(20);index:date_idx;comment:统计的日期"`
	Bu          string `json:"bu" gorm:"type:varchar(64);comment:bu"`
	ClusterID   string `json:"cluster_id" gorm:"type:varchar(64);index:idx_cluster_id;not null;comment:集群ID"`
	ClusterName string `json:"cluster_name" gorm:"type:varchar(64);index:idx_cluster_name;not null;comment:集群名称"`
	Database    string `json:"database" gorm:"type:varchar(64);index:idx_database;not null;comment:库名"`
	Table       string `json:"table" gorm:"type:varchar(64);comment:表名"`
	TableSize   int    `json:"table_size" gorm:"type:int;comment:表大小(GB)"`
	Policies    string `json:"policies" gorm:"type:longtext;comment:对应的策略"` // 不包含重建表的策略
}
