package email

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"github.com/sunkaimr/data-loom/configs"
	. "github.com/sunkaimr/data-loom/internal/notice/message"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"gopkg.in/gomail.v2"
	"html/template"
)

type Email struct {
	Host               string
	Port               int
	Username           string
	Password           string
	InsecureSkipVerify bool
}

func (c *Email) Send(_ *common.Context, msg *Message) error {
	tmpl, err := template.New("template").Parse(emailTemplate)
	if err != nil {
		return err
	}

	b := bytes.Buffer{}
	err = tmpl.Execute(&b, msg)
	if err != nil {
		return err
	}

	m := gomail.NewMessage()
	m.SetHeader("From", c.Username)
	m.SetHeader("To", msg.Relevant...)
	m.SetHeader("Subject", "MySQL数据治理平台")
	m.SetBody("text/html", b.String())

	d := gomail.NewDialer(c.Host, c.Port, c.Username, c.Password)
	d.TLSConfig = &tls.Config{InsecureSkipVerify: c.InsecureSkipVerify}
	return d.DialAndSend(m)
}

func (c *Email) Test(_ *common.Context, testUser string) error {
	tmpl, err := template.New("template").Parse(emailTemplate)
	if err != nil {
		return err
	}

	msg := Message{
		TaskID:             123,
		TaskName:           "这是一个任务通知示例",
		SrcClusterName:     "cluster-name",
		SrcDatabaseName:    "database",
		SrcTablesName:      "table",
		Govern:             "删除数据",
		Condition:          "id < 10000",
		TaskStartTime:      "2024-01-01 00:00:00",
		TaskEndTime:        "2024-01-01 01:00:00",
		TaskDuration:       "01:00:00",
		TaskStatus:         "执行成功",
		TaskResultQuantity: 123,
		TaskResultSize:     456,
		TaskReason:         "",

		HomeURL: configs.C.Server.ExternalAddr,
		TaskURL: fmt.Sprintf("%s/#/task/detail?task_id=", configs.C.Server.ExternalAddr),
	}

	b := bytes.Buffer{}
	err = tmpl.Execute(&b, msg)
	if err != nil {
		return err
	}

	m := gomail.NewMessage()
	m.SetHeader("From", c.Username)
	m.SetHeader("To", testUser)
	m.SetHeader("Subject", "MySQL数据治理平台")
	m.SetBody("text/html", b.String())

	d := gomail.NewDialer(c.Host, c.Port, c.Username, c.Password)
	d.TLSConfig = &tls.Config{InsecureSkipVerify: c.InsecureSkipVerify}
	return d.DialAndSend(m)
}

const emailTemplate = `
	<body>
		<div style="width:100%; height: 100%; position: relative;">
			<div style="width: 800px; height: 100%; position: absolute; margin: auto; top: 0; left: 0; right: 0; bottom: 0;">
				<table border="1px" cellspacing="0" style="width: 800px; font-family: Microsoft YaHei; font-size: 14px; line-height: 2; border: 1px solid #CCC; border-collapse: collapse; overflow: auto;">
					<caption style="font-size: 20px; font-weight: bolder; margin: 20px;">MySQL数据治理平台 - 任务通知</caption>
					<tbody>
						<tr>
							<td style="width: 200px; text-align: right; padding-right: 20px; font-weight: bolder;">任务ID</td>
							<td style="width: 600px; text-align: left; padding-left: 20px;">
								<a href="{{ .TaskURL }}" target="_blank" style="text-decoration: none; color: #9981e8;">{{ .TaskID }}</a>
							</td>
						</tr>
						<tr>
							<td style="text-align: right; padding-right: 20px; font-weight: bolder;">任务名称</td>
							<td style="text-align: left; padding-left: 20px;">{{ .TaskName }}</td>
						</tr>
						<tr>
							<td style="text-align: right; padding-right: 20px; font-weight: bolder;">集群名称</td>
							<td style="text-align: left; padding-left: 20px;">{{ .SrcClusterName }}</td>
						</tr>
	
						<tr>
							<td style="text-align: right; padding-right: 20px; font-weight: bolder;">库名</td>
							<td style="text-align: left; padding-left: 20px;">{{ .SrcDatabaseName }}</td>
						</tr>
						<tr>
							<td style="text-align: right; padding-right: 20px; font-weight: bolder;">表名</td>
							<td style="text-align: left; padding-left: 20px;">{{ .SrcTablesName }}</td>
						</tr>
						<tr>
							<td style="text-align: right; padding-right: 20px; font-weight: bolder;">治理方式</td>
							<td style="text-align: left; padding-left: 20px;">{{ .Govern }}</td>
						</tr>
						<tr>
							<td style="text-align: right; padding-right: 20px; font-weight: bolder;">治理条件</td>
							<td style="text-align: left; padding-left: 20px;">{{ .Condition }}</td>
						</tr>
						<tr>
							<td style="text-align: right; padding-right: 20px; font-weight: bolder;">任务开始时间</td>
							<td style="text-align: left; padding-left: 20px;">{{ .TaskStartTime }}</td>
						</tr>
						<tr>
							<td style="text-align: right; padding-right: 20px; font-weight: bolder;">任务结束时间</td>
							<td style="text-align: left; padding-left: 20px;">{{ .TaskEndTime }}</td>
						</tr>
						<tr>
							<td style="text-align: right; padding-right: 20px; font-weight: bolder;">任务执行时长</td>
							<td style="text-align: left; padding-left: 20px;">{{ .TaskDuration }}</td>
						</tr>
						<tr style="background-color: {{ .TaskStatusColor }}">
							<td style="text-align: right; padding-right: 20px; font-weight: bolder;">任务状态</td>
							<td style="text-align: left; padding-left: 20px;">{{ .TaskStatus }}</td>
						</tr>
						<tr>
							<td style="text-align: right; padding-right: 20px; font-weight: bolder;">治理数据行数</td>
							<td style="text-align: left; padding-left: 20px;">{{ .TaskResultQuantity }}</td>
						</tr>
						<tr>
							<td style="text-align: right; padding-right: 20px; font-weight: bolder;">治理数据大小(MB)</td>
							<td style="text-align: left; padding-left: 20px;">{{ .TaskResultSize }}</td>
						</tr>
						<tr>
							<td style="text-align: right; padding-right: 20px; font-weight: bolder;">原因</td>
							<td style="text-align: left; padding-left: 20px;">{{ .TaskReason }}</td>
						</tr>
					</tbody>
				</table>
				<div style="height: 50px;text-align: center;">
					<p style="font-size: 12px; font-style: italic; color: #ccc;">本邮件由系统自动发出请勿回复，更多信息请登录<a href="{{ .HomeURL }}" target="_blank" style="text-decoration: none; color: #9981e8;">平台</a>查看</p>
				</div>
			</div>
		</div>
	</body>
`
