package models

type Source struct {
	Model

	// 源端信息
	Name         string `json:"name" gorm:"type:varchar(1024);not null;comment:源端名称"`
	Description  string `json:"description" gorm:"type:longtext;default '';comment:说明"`
	Bu           string `json:"bu" gorm:"type:varchar(64);comment:资产BU"`
	ClusterName  string `json:"cluster_name" gorm:"type:varchar(128);not null;comment:集群名称"`
	ClusterID    string `json:"cluster_id" gorm:"type:varchar(128);not null;index:cluster_idx;comment:集群ID"`
	DatabaseName string `json:"database_name" gorm:"type:varchar(128);index:database_name_idx;comment:源库名"`
	TablesName   string `json:"tables_name" gorm:"type:longtext;comment:源表名"`
}
