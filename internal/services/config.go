package services

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sunkaimr/data-loom/configs"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/pkg/utils"
)

var Cfg = &ConfigService{}

type ConfigService struct {
	ClusterDefaultUser     string `json:"cluster_default_user"`     // 源端用户名
	ClusterDefaultPasswd   string `json:"cluster_default_passwd"`   // 源端密码
	ClusterExcludeDatabase string `json:"cluster_exclude_database"` // 源端库黑名单
	ClusterExcludeTables   string `json:"cluster_exclude_tables"`   // 源端表黑名单

	TaskMaxParallel       int                          `json:"task_max_parallel"`       // 任务的最大并法数
	TaskTimeout           int                          `json:"task_timeout"`            // 任务的超时时间
	TaskConflictLevel     common.TaskConflictLevelType `json:"task_conflict_level"`     // 任务冲突级别：源端集群级别，库级别，表级别
	TaskConflictMax       int                          `json:"task_conflict_max"`       // 最多允许运行几个任务
	WorkflowRetentionDays int                          `json:"workflow_retention_days"` // 任务的工作流保留天数
	ThanosUrl             string                       `json:"thanos_url"`              // 监控指标地址

	SourceStatusDetect          bool `json:"source_status_detect"`            // 源端库运行状态检测
	SourceStatusDetectDiskUsage int  `json:"source_status_detect_disk_usage"` // 源端磁盘使用率

	// 通知相关
	Notice                  common.NoticeType `json:"notice"`
	EmailHost               string            `json:"email_host"`
	EmailPort               int               `json:"email_port"`
	EmailUsername           string            `json:"email_username"`
	EmailPassword           string            `json:"email_password"`
	EmailInsecureSkipVerify bool              `json:"email_insecure_skip_verify"`
}

func (_ *ConfigService) ReloadConfig(ctx *gin.Context) {
	log, _ := common.ExtractContext(ctx)
	_, _, err := new(ConfigService).GetConfig(ctx)
	if err != nil {
		log.Errorf("reload config from db failed, %s", err)
		return
	}
}

// GetConfig 查询配置
func (c *ConfigService) GetConfig(ctx *gin.Context) (*ConfigService, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	cfg := &models.Config{ID: 1}
	res := db.Model(cfg).Where("id=1").First(cfg)
	if res.Error != nil {
		err := fmt.Errorf("query cfg from db failed, %s", res.Error)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	Cfg.ModelToService(cfg)

	return Cfg, common.CodeOK, nil
}

// UpdateConfig 更新配置
func (c *ConfigService) UpdateConfig(ctx *gin.Context) (*ConfigService, common.ServiceCode, error) {
	var err error
	log, db := common.ExtractContext(ctx)

	cfg := &models.Config{ID: 1}
	res := db.Model(cfg).Where("id=1").First(cfg)
	if res.Error != nil {
		err := fmt.Errorf("query cfg from db failed, %s", res.Error)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	if c.ClusterDefaultPasswd != cfg.ClusterDefaultPasswd {
		encryptedPasswd, err := utils.EncryptByAES(c.ClusterDefaultPasswd, configs.C.Jwt.Secret)
		if err != nil {
			err = fmt.Errorf("encrypt passwd failed, %s", err)
			log.Error(err)
			return nil, common.CodeEncryptPasswdErr, err
		}

		c.ClusterDefaultPasswd = encryptedPasswd
	}

	c.ClusterExcludeDatabase = utils.TrimmingStringList(c.ClusterExcludeDatabase, ",")
	c.ClusterExcludeTables = utils.TrimmingStringList(c.ClusterExcludeTables, ",")

	if c.TaskConflictLevel != "" && !common.CheckTaskConflictLevel(c.TaskConflictLevel) {
		return nil, common.CodeConfigConflictLevelErr, fmt.Errorf("TaskConflictLevel period(%s) not pass", c.TaskConflictLevel)
	} else if c.TaskConflictLevel == "" {
		c.TaskConflictLevel = cfg.TaskConflictLevel
	}

	if c.TaskConflictMax <= 0 {
		c.TaskConflictMax = cfg.TaskConflictMax
	}

	if c.WorkflowRetentionDays <= 0 {
		c.WorkflowRetentionDays = cfg.WorkflowRetentionDays
	}

	cfg = c.ServiceToModel()

	err = db.Where("id=1").Save(cfg).Error
	if err != nil {
		err = fmt.Errorf("save cfg to db failed, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	Cfg.ModelToService(cfg)

	log.Infof("update cfg(%+v) success", cfg)
	return Cfg, common.CodeOK, nil
}

func (c *ConfigService) ServiceToModel() *models.Config {
	m := &models.Config{}
	m.ID = 1
	m.ClusterDefaultUser = c.ClusterDefaultUser
	m.ClusterDefaultPasswd = c.ClusterDefaultPasswd
	m.ClusterExcludeDatabase = c.ClusterExcludeDatabase
	m.ClusterExcludeTables = c.ClusterExcludeTables
	m.ThanosUrl = c.ThanosUrl
	m.TaskMaxParallel = c.TaskMaxParallel
	m.TaskTimeout = c.TaskTimeout
	m.TaskConflictLevel = c.TaskConflictLevel
	m.TaskConflictMax = c.TaskConflictMax
	m.WorkflowRetentionDays = c.WorkflowRetentionDays
	m.Notice = c.Notice
	m.EmailHost = c.EmailHost
	m.EmailPort = c.EmailPort
	m.EmailUsername = c.EmailUsername
	m.EmailPassword = c.EmailPassword
	m.EmailInsecureSkipVerify = c.EmailInsecureSkipVerify

	m.SourceStatusDetect = c.SourceStatusDetect
	m.SourceStatusDetectDiskUsage = c.SourceStatusDetectDiskUsage
	return m
}

func (c *ConfigService) ModelToService(m *models.Config) *ConfigService {
	c.ClusterDefaultUser = m.ClusterDefaultUser
	c.ClusterDefaultPasswd = m.ClusterDefaultPasswd
	c.ClusterExcludeDatabase = m.ClusterExcludeDatabase
	c.ClusterExcludeTables = m.ClusterExcludeTables
	c.ThanosUrl = m.ThanosUrl
	c.TaskMaxParallel = m.TaskMaxParallel
	c.TaskTimeout = m.TaskTimeout
	c.TaskConflictLevel = m.TaskConflictLevel
	c.TaskConflictMax = m.TaskConflictMax
	c.WorkflowRetentionDays = m.WorkflowRetentionDays
	c.Notice = m.Notice
	c.EmailHost = m.EmailHost
	c.EmailPort = m.EmailPort
	c.EmailUsername = m.EmailUsername
	c.EmailPassword = m.EmailPassword
	c.EmailInsecureSkipVerify = m.EmailInsecureSkipVerify
	c.SourceStatusDetect = m.SourceStatusDetect
	c.SourceStatusDetectDiskUsage = m.SourceStatusDetectDiskUsage
	return c
}
