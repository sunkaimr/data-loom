package common

import (
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"strconv"
)

type PageList[T any] struct {
	PageSize int   `json:"pageSize"` // 每页大小
	Page     int   `json:"page"`     // 当前页数
	Total    int64 `json:"total"`    // 总数据量
	Items    T     `json:"items"`    // 数据
	db       *gorm.DB
	sel      string
	order    string
	err      error
}

func NewPageList[T any](db *gorm.DB) *PageList[T] {
	var data T
	return &PageList[T]{
		Items: data,
		db:    db,
	}
}
func (p *PageList[any]) DB() *gorm.DB {
	return p.db
}

func (p *PageList[any]) QueryPaging(ctx *gin.Context) *PageList[any] {
	page, _ := strconv.Atoi(ctx.Query("page"))
	pageSize, _ := strconv.Atoi(ctx.Query("pageSize"))

	if page == 0 {
		page = 1
	}

	if pageSize == 0 {
		pageSize = 10
	} else if pageSize < 0 {
		pageSize = -1
	}

	p.Page, p.PageSize = page, pageSize
	return p
}

func (p *PageList[any]) Paging(f func() (int, int)) *PageList[any] {
	page, pageSize := f()
	if page == 0 {
		page = 1
	}

	if pageSize == 0 {
		pageSize = 10
	} else if pageSize < 0 {
		pageSize = -1
	}
	p.Page, p.PageSize = page, pageSize
	return p
}

func (p *PageList[any]) Select(s string) *PageList[any] {
	p.sel = s
	return p
}

func (p *PageList[any]) Order(s string) *PageList[any] {
	p.order = s
	return p
}

func (p *PageList[any]) Query(scopes ...func(*gorm.DB) *gorm.DB) (*PageList[any], error) {
	if p.sel == "" {
		p.sel = "*"
	}
	if p.order == "" {
		p.order = "id asc"
	}
	offset := (p.Page - 1) * p.PageSize
	err := p.db.
		Model(p.Items).
		Select(p.sel).
		Scopes(scopes...).
		Count(&p.Total).
		Order(p.order).
		Offset(offset).
		Limit(p.PageSize).
		Find(&p.Items).Error
	return p, err
}
