package common

import (
	"fmt"
	"gorm.io/gorm"
	"strings"
)

// FilterFuzzyString 模糊过滤字符串
func FilterFuzzyString(name string, val string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if name == "" || val == "" {
			return db
		}
		return db.Where(fmt.Sprintf("%s like ?", name), "%"+val+"%")
	}
}

// FilterFuzzyStringMap 多个模糊过滤字符串
func FilterFuzzyStringMap(m map[string]string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		for k, v := range m {
			db = FilterFuzzyString(k, v)(db)
		}
		return db
	}
}

func FilterID(id uint) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if InvalidUintID(id) {
			return db
		}
		return db.Where("id =?", id)
	}
}

// FilterCustomUintID 通过自定义名字筛选uint类型的值
func FilterCustomUintID(name string, id uint) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if InvalidUintID(id) || name == "" {
			return db
		}
		return db.Where(fmt.Sprintf("`%s` =?", name), id)
	}
}

// FilterCustomIntID 通过自定义名字筛选uint类型的值
func FilterCustomIntID(name string, id int) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if InvalidIntID(id) || name == "" {
			return db
		}
		return db.Where(fmt.Sprintf("`%s` =?", name), id)
	}
}

// FilterCustomBool 支持bool类型筛选
func FilterCustomBool(name string, value interface{}, filter bool) func(db *gorm.DB) *gorm.DB {
	switch v := value.(type) {
	case string:
		switch strings.ToLower(v) {
		case "true":
			value = true
		default:
			value = false
		}
	case bool:
		value = v
	case int, uint:
		switch v {
		case 0:
			value = false
		default:
			value = true
		}
	default:
		filter = false
	}

	return func(db *gorm.DB) *gorm.DB {
		if name == "" || !filter {
			return db
		}
		return db.Where(fmt.Sprintf("`%s` =?", name), value)
	}
}

func FilterDataRange(name, start, end string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if name == "" || (start == "" && end == "") {
			return db
		}
		if start == "" {
			return db.Where(fmt.Sprintf("`%s` <= ?", name), end)
		}
		if end == "" {
			return db.Where(fmt.Sprintf("`%s` >= ?", name), start)
		}
		return db.Where(fmt.Sprintf("`%s` >= ? AND `%s` <= ?", name, name), start, end)
	}
}

// FilterMultiCondition 多条件过滤
func FilterMultiCondition(key string, values []string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if key == "" || len(values) == 0 {
			return db
		}
		return db.Where(fmt.Sprintf("`%s` IN (?)", key), values)
	}
}
