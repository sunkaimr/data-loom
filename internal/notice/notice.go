package notice

import (
	"encoding/json"
	"fmt"
	"github.com/sunkaimr/data-loom/configs"
	"github.com/sunkaimr/data-loom/internal/models"
	. "github.com/sunkaimr/data-loom/internal/notice/message"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/pkg/email"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"strings"
	"time"
)

type Notice interface {
	// Send 发送通知
	Send(*common.Context, *Message) error

	// Test 通知测试
	Test(*common.Context, string /*测试用户地址*/) error
}

func NewDriver(c *models.Config) Notice {
	switch c.Notice {
	case common.NoticeTypeEmail:
		return &email.Email{
			Host:               c.EmailHost,
			Port:               c.EmailPort,
			Username:           c.EmailUsername,
			Password:           c.EmailPassword,
			InsecureSkipVerify: c.EmailInsecureSkipVerify,
		}
	default:
		return &email.Email{
			Host:               c.EmailHost,
			Port:               c.EmailPort,
			Username:           c.EmailUsername,
			Password:           c.EmailPassword,
			InsecureSkipVerify: c.EmailInsecureSkipVerify,
		}
	}
}

func GenerateMessage(ctx *common.Context, task *models.Task) *Message {
	log, db := ctx.Log, ctx.DB

	msg := &Message{
		TaskID:             task.ID,
		TaskName:           task.Name,
		SrcClusterName:     task.SrcClusterName,
		SrcDatabaseName:    task.SrcDatabaseName,
		Govern:             common.GovernCN[task.Govern],
		Condition:          task.Condition,
		TaskStartTime:      task.TaskStartTime.Format(time.DateTime),
		TaskEndTime:        task.TaskEndTime.Format(time.DateTime),
		TaskDuration:       utils.HumanFormatTimeSeconds(task.TaskDuration),
		TaskStatus:         common.TaskStatusCN[task.TaskStatus],
		TaskResultQuantity: task.TaskResultQuantity,
		TaskResultSize:     task.TaskResultSize,
		TaskReason:         task.TaskReason,

		HomeURL: configs.C.Server.ExternalAddr,
		TaskURL: fmt.Sprintf("%s/#/task/detail?task_id=%d", configs.C.Server.ExternalAddr, task.ID),
	}

	if task.TaskStatus == common.TaskStatusSupplementFailed ||
		task.TaskStatus == common.TaskStatusExecCheckFailed ||
		task.TaskStatus == common.TaskStatusExecFailed ||
		task.TaskStatus == common.TaskStatusTimeout {
		msg.TaskStatusColor = "#F33"
	}

	if b, table, err := common.CheckSameShardingTables(task.SrcTablesName); err == nil {
		if b {
			msg.SrcTablesName = table + fmt.Sprintf("【共%d张分表】", len(strings.Split(task.SrcTablesName, ",")))
		} else {
			msg.SrcTablesName = table
		}
	}
	var relevant []string
	_ = json.Unmarshal(task.Relevant, &relevant)
	for i, v := range relevant {
		if v == "" || utils.IsMail(v) {
			continue
		}

		var u models.User
		err := db.Model(models.User{}).Where("username =? OR real_name =?", v, v).First(&u).Error
		if err != nil {
			log.Errorf("query models.User(username=%v OR real_name=%v) failed, %s", v, v, err)
			continue
		}
		relevant[i] = u.Email
	}
	msg.Relevant = relevant
	return msg
}
