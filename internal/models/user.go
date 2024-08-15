package models

import "time"

type User struct {
	Model
	Username  string    `json:"username" gorm:"type:varchar(50);not null;index:user_idx"`
	Password  string    `json:"password" gorm:"type:varchar(150);not null"`
	RealName  string    `json:"real_name" gorm:"type:varchar(50);"`
	Email     string    `json:"email" gorm:"type:varchar(50);"`
	IsLdap    uint      `json:"is_ldap" gorm:"type:tinyint(2) not null default 0"`
	LastLogin time.Time `json:"last_login" gorm:"type:DATETIME;"`
}
