package services

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"reflect"
	"time"
)

type PolicyRevisionService struct {
	Model
	PolicyID    uint   `json:"policy_id"`    // 策略ID
	ModifyField string `json:"modify_field"` // 修改字段
	OldValue    string `json:"old_value"`    // 原始值
	NewValue    string `json:"new_value"`    // 修改值
}

type TaskRevisionService struct {
	Model
	TaskID      uint   `json:"task_id"`      // 任务ID
	ModifyField string `json:"modify_field"` // 修改字段
	OldValue    string `json:"old_value"`    // 原始值
	NewValue    string `json:"new_value"`    // 修改值
}

func (c *PolicyRevisionService) CreatePolicyRevision(ctx *gin.Context, old, new *models.Policy) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	revisions := compareFields((&PolicyService{}).ModelToService(old), (&PolicyService{}).ModelToService(new))
	if len(revisions) == 0 {
		return common.CodeOK, nil
	}

	revisionsModel := make([]*models.PolicyRevision, 0, len(revisions))
	for i := 0; i < len(revisions); i++ {
		revisionsModel = append(revisionsModel, &models.PolicyRevision{
			Model: models.Model{
				Creator: u.UserName,
			},
			PolicyID:    old.ID,
			ModifyField: revisions[i][0],
			OldValue:    revisions[i][1],
			NewValue:    revisions[i][2],
		})
	}

	err := db.Create(revisionsModel).Error
	if err != nil {
		err = fmt.Errorf("save job revisions failed, %s", err)
		log.Error(err)
		return common.CodeServerErr, err
	}
	return common.CodeOK, nil
}

func (c *PolicyRevisionService) QueryPolicyRevision(ctx *gin.Context, queryMap map[string]string) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	res, err := common.NewPageList[[]models.PolicyRevision](db).
		QueryPaging(ctx).
		Order("id desc").
		Query(
			common.FilterFuzzyStringMap(queryMap),
			common.FilterID(c.ID),
			common.FilterCustomUintID("policy_id", c.PolicyID),
		)
	if err != nil {
		err = fmt.Errorf("query models.PolicyRevision from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	ret := common.NewPageList[[]PolicyRevisionService](db)
	ret.Page = res.Page
	ret.PageSize = res.PageSize
	ret.Total = res.Total
	for i := range res.Items {
		s := &PolicyRevisionService{}
		s.ModelToService(&res.Items[i])
		ret.Items = append(ret.Items, *s)
	}

	return ret, common.CodeOK, nil
}

func (c *TaskRevisionService) CreateTaskRevision(ctx *gin.Context, old, new *models.Task) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	revisions := compareFields((&TaskService{}).ModelToService(old), (&TaskService{}).ModelToService(new))
	if len(revisions) == 0 {
		return common.CodeOK, nil
	}

	revisionsModel := make([]*models.TaskRevision, 0, len(revisions))
	for i := 0; i < len(revisions); i++ {
		revisionsModel = append(revisionsModel, &models.TaskRevision{
			Model: models.Model{
				Creator: u.UserName,
			},
			TaskID:      old.ID,
			ModifyField: revisions[i][0],
			OldValue:    revisions[i][1],
			NewValue:    revisions[i][2],
		})
	}

	err := db.Create(revisionsModel).Error
	if err != nil {
		err = fmt.Errorf("save task revisions failed, %s", err)
		log.Error(err)
		return common.CodeServerErr, err
	}
	return common.CodeOK, nil
}

func (c *TaskRevisionService) QueryTaskRevision(ctx *gin.Context, queryMap map[string]string) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	res, err := common.NewPageList[[]models.TaskRevision](db).
		QueryPaging(ctx).
		Order("id desc").
		Query(
			common.FilterFuzzyStringMap(queryMap),
			common.FilterID(c.ID),
			common.FilterCustomUintID("task_id", c.TaskID),
		)
	if err != nil {
		err = fmt.Errorf("query models.TaskRevision from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	ret := common.NewPageList[[]TaskRevisionService](db)
	ret.Page = res.Page
	ret.PageSize = res.PageSize
	ret.Total = res.Total
	for i := range res.Items {
		s := &TaskRevisionService{}
		s.ModelToService(&res.Items[i])
		ret.Items = append(ret.Items, *s)
	}

	return ret, common.CodeOK, nil
}

func (c *PolicyRevisionService) ServiceToModel() *models.PolicyRevision {
	m := &models.PolicyRevision{}
	m.ID = c.ID
	m.Creator = c.Creator
	m.Editor = c.Editor
	m.PolicyID = c.PolicyID
	m.ModifyField = c.ModifyField
	m.OldValue = c.OldValue
	m.NewValue = c.NewValue
	m.CreatedAt, _ = time.ParseInLocation(time.DateTime, c.CreatedAt, time.Now().Location())
	m.UpdatedAt, _ = time.ParseInLocation(time.DateTime, c.UpdatedAt, time.Now().Location())
	return m
}

func (c *PolicyRevisionService) ModelToService(m *models.PolicyRevision) *PolicyRevisionService {
	c.ID = m.ID
	c.CreatedAt = m.CreatedAt.Format(time.DateTime)
	c.UpdatedAt = m.UpdatedAt.Format(time.DateTime)
	c.Creator = m.Creator
	c.Editor = m.Editor
	c.PolicyID = m.PolicyID
	c.ModifyField = m.ModifyField
	c.OldValue = m.OldValue
	c.NewValue = m.NewValue
	return c
}

func compareFields(old, new interface{}) [][3]string {
	var revisions [][3]string
	compareNestedFields(reflect.ValueOf(old).Elem(), reflect.ValueOf(new).Elem(), reflect.TypeOf(old).Elem(), &revisions, "")
	return revisions
}

func compareNestedFields(oldValue, newValue reflect.Value, oldType reflect.Type, revisions *[][3]string, prefix string) {
	for i := 0; i < oldValue.NumField(); i++ {
		field := oldType.Field(i)
		fieldName := prefix + field.Name
		oldFieldValue := oldValue.Field(i).Interface()
		newFieldValue := newValue.Field(i).Interface()

		// 如果是嵌套的结构体或指针指向结构体，则递归比较
		if oldValue.Field(i).Kind() == reflect.Struct || (oldValue.Field(i).Kind() == reflect.Ptr && oldValue.Field(i).Elem().Kind() == reflect.Struct) {
			var oldNestedValue, newNestedValue reflect.Value
			if oldValue.Field(i).Kind() == reflect.Ptr {
				oldNestedValue = oldValue.Field(i).Elem()
				newNestedValue = newValue.Field(i).Elem()
			} else {
				oldNestedValue = oldValue.Field(i)
				newNestedValue = newValue.Field(i)
			}
			compareNestedFields(oldNestedValue, newNestedValue, field.Type, revisions, fieldName+".")
			continue
		}

		if utils.ElementExist(field.Name, []string{"CreatedAt", "UpdatedAt", "Creator", "Editor"}) {
			continue
		}

		// 如果值不相等，添加到修订记录中
		if !reflect.DeepEqual(oldFieldValue, newFieldValue) {
			*revisions = append(*revisions, [3]string{
				fieldName,
				fmt.Sprintf("%v", oldFieldValue),
				fmt.Sprintf("%v", newFieldValue),
			})
		}
	}
}

func (c *TaskRevisionService) ServiceToModel() *models.TaskRevision {
	m := &models.TaskRevision{}
	m.ID = c.ID
	m.Creator = c.Creator
	m.Editor = c.Editor
	m.CreatedAt, _ = time.ParseInLocation(time.DateTime, c.CreatedAt, time.Now().Location())
	m.UpdatedAt, _ = time.ParseInLocation(time.DateTime, c.UpdatedAt, time.Now().Location())
	m.TaskID = c.TaskID
	m.ModifyField = c.ModifyField
	m.OldValue = c.OldValue
	m.NewValue = c.NewValue
	return m
}

func (c *TaskRevisionService) ModelToService(m *models.TaskRevision) *TaskRevisionService {
	c.ID = m.ID
	c.Creator = m.Creator
	c.Editor = m.Editor
	c.CreatedAt = m.CreatedAt.Format(time.DateTime)
	c.UpdatedAt = m.UpdatedAt.Format(time.DateTime)
	c.TaskID = m.TaskID
	c.ModifyField = m.ModifyField
	c.OldValue = m.OldValue
	c.NewValue = m.NewValue
	return c
}
