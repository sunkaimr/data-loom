package services

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/notice"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"time"
)

// TaskChangeLogService 任务的执行过程记录
type TaskChangeLogService struct {
	ID         uint                  `json:"id"`
	TaskID     uint                  `json:"task_id"`     // 任务ID
	Time       string                `json:"time"`        // 创建时间
	UserName   string                `json:"user_name"`   // 创建人
	TaskStatus common.TaskStatusType `json:"task_status"` // 任务状态
	Content    string                `json:"content"`     // 变化内容记录
}

func CreateTaskChangeLog(ctx *common.Context, task *models.Task, user, content string) {
	log, db := ctx.Log, ctx.DB

	db.Save(&models.TaskChangeLog{
		TaskID:     task.ID,
		TaskStatus: task.TaskStatus,
		Time:       time.Now(),
		UserName:   user,
		Content:    content,
	})

	needSend := false
	switch task.NotifyPolicy {
	case common.NotifyPolicyTypeSilence:
		return
	case common.NotifyPolicyTypeSuccess:
		// 执行成功通知
		if task.TaskStatus == common.TaskStatusSupplementFailed ||
			task.TaskStatus == common.TaskStatusExecCheckFailed ||
			task.TaskStatus == common.TaskStatusSuccess {
			needSend = true
		}

	case common.NotifyPolicyTypeFailed:
		// 执行失败通知
		if task.TaskStatus == common.TaskStatusSupplementFailed ||
			task.TaskStatus == common.TaskStatusExecCheckFailed ||
			task.TaskStatus == common.TaskStatusExecFailed ||
			task.TaskStatus == common.TaskStatusTimeout {
			needSend = true
		}
	case common.NotifyPolicyTypeAlways:
		// 所有情况都通知
		if task.TaskStatus == common.TaskStatusSupplementFailed ||
			task.TaskStatus == common.TaskStatusExecCheckFailed ||
			task.TaskStatus == common.TaskStatusExecFailed ||
			task.TaskStatus == common.TaskStatusSuccess ||
			task.TaskStatus == common.TaskStatusTimeout {
			needSend = true
		}
	}

	if needSend {
		msg := notice.GenerateMessage(ctx, task)
		err := notice.NewDriver(Cfg.ServiceToModel()).Send(ctx, msg)
		if err != nil {
			log.Errorf("send notice(%+v) failed, %s", msg, err)
		} else {
			log.Debugf("send notice(%+v) success", msg)
		}
	}
}

func (c *TaskChangeLogService) QueryTaskChangeLog(ctx *gin.Context) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	res, err := common.NewPageList[[]models.TaskChangeLog](db).
		QueryPaging(ctx).
		Query(
			common.FilterCustomUintID("task_id", c.TaskID),
		)
	if err != nil {
		err = fmt.Errorf("query models.TaskChangeLog from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	ret := common.NewPageList[[]TaskChangeLogService](db)
	ret.Page = res.Page
	ret.PageSize = res.PageSize
	ret.Total = res.Total
	for i := range res.Items {
		s := &TaskChangeLogService{}
		s.ModelToService(&res.Items[i])
		ret.Items = append(ret.Items, *s)
	}

	return ret, common.CodeOK, nil
}

func (c *TaskChangeLogService) ServiceToModel() *models.TaskChangeLog {
	m := &models.TaskChangeLog{}
	m.ID = c.ID
	m.TaskID = c.TaskID
	m.TaskStatus = c.TaskStatus
	m.Time, _ = time.ParseInLocation(time.DateTime, c.Time, time.Now().Location())
	m.UserName = c.UserName
	m.Content = c.Content
	return m
}

func (c *TaskChangeLogService) ModelToService(m *models.TaskChangeLog) *TaskChangeLogService {
	c.ID = m.ID
	c.TaskID = m.TaskID
	c.TaskStatus = m.TaskStatus
	c.Time = m.Time.Format(time.DateTime)
	c.UserName = m.UserName
	c.Content = m.Content
	return c
}
