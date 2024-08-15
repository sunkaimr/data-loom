package services

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	. "github.com/sunkaimr/data-loom/internal/job/status"
	"github.com/sunkaimr/data-loom/internal/models"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	. "github.com/sunkaimr/data-loom/internal/pkg/queue"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"gorm.io/gorm"
	"strings"
	"time"
)

type PolicyService struct {
	// 归档策略元数据信息
	Model
	Name          string                   `json:"name"`           // 策略名称
	Description   string                   `json:"description"`    // 说明
	Bu            string                   `json:"bu"`             // 资产BU
	Enable        bool                     `json:"enable"`         // 是否生效
	Period        common.PeriodType        `json:"period"`         // 执行周期 执行一次:once, 每月一次:monthly, 每季度一次:quarterly, 每半年一次:six-months, 每年一次:yearly
	Day           int                      `json:"day"`            // 期望在每月的几号执行，对于执行周期小于一个月的任务不生效
	ExecuteWindow []string                 `json:"execute_window"` // 执行时间窗口 如: [] 或者 ["03:00:00", "05:00:00"]
	Pause         bool                     `json:"pause"`          // 执行窗口外是否需要暂停执行
	RebuildFlag   bool                     `json:"rebuild_flag"`   // 执行窗口外是否重建表(仅在治理方式是删除时有效)。true:在执行窗口外仍然走重建流程; false:执行窗口外跳过重建流程
	CleaningSpeed common.CleaningSpeedType `json:"cleaning_speed"` // 清理速度 稳定优先:steady, 速度适中:balanced, 速度优先:swift

	// 源端信息
	SrcID           uint   `json:"src_id"`            // 源端ID
	SrcName         string `json:"src_name"`          // 源端名称
	SrcClusterName  string `json:"src_cluster_name"`  // 集群名称
	SrcClusterID    string `json:"src_cluster_id"`    // 集群ID
	SrcDatabaseName string `json:"src_database_name"` // 源库名
	SrcTablesName   string `json:"src_tables_name"`   // 源表名

	// 数据治理方式
	Govern        common.GovernType `json:"govern"`          // 数据治理方式 清空数据:truncate, 不备份清理:delete, 备份后清理:backup-delete, 归档:archive, 重建表：recreate
	Condition     string            `json:"condition"`       // 数据治理条件, where条件
	ArchiveScope  string            `json:"archive_scope"`   // 数据归档范围，归档那些列（仅归档涉及）
	RetainSrcData bool              `json:"retain_src_data"` // 归档时否保留源表数据默认不保留（仅归档涉及）

	// 目标端信息
	DestID       uint `json:"dest_id"`       // 目标端ID
	ForceArchive bool `json:"force_archive"` // 当目标库、表都存在时强制向目标端归档

	// 结果通知
	Relevant     []string                `json:"relevant"`      // 关注人
	NotifyPolicy common.NotifyPolicyType `json:"notify_policy"` // 通知策略 不通知:silence, 成功时通知:success, 失败时通知:failed, 成功或失败都通知:always
}

func (c *PolicyService) CheckParameters(ctx *gin.Context) (bool, common.ServiceCode, error) {
	_, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	c.Relevant = utils.RemoveDupElement(append(c.Relevant, u.UserName))

	if !common.CheckPeriod(c.Period) {
		return false, common.CodePolicyPeriodErr, fmt.Errorf("validate period(%s) not pass", c.Period)
	}

	// 当执行周期大于月度时需要检查day是否合理
	if !common.CheckPolicyDay(c.Period, c.Day) {
		return false, common.CodePolicyDayErr,
			fmt.Errorf("validate day(%v) not pass, day should in (1-31) when policy.Period greater than one month", c.Day)
	}

	if !common.CheckGovernType(c.Govern) {
		return false, common.CodePolicyGovernErr, fmt.Errorf("validate govern(%s) not pass", c.Govern)
	}

	if c.CleaningSpeed == "" {
		c.CleaningSpeed = common.CleaningSpeedDefault
	} else if !common.CheckCleaningSpeed(c.CleaningSpeed) {
		return false, common.CodePolicyCleaningSpeedErr, fmt.Errorf("validate cleaning_speed(%s) not pass", c.CleaningSpeed)
	}

	if c.NotifyPolicy == "" {
		c.NotifyPolicy = common.NotifyPolicyTypeDefault
	} else if !common.CheckNotifyPolicyType(c.NotifyPolicy) {
		return false, common.CodePolicyNotifyPolicyErr, fmt.Errorf("validate notify_policy(%s) not pass", c.NotifyPolicy)
	}

	_, execWin, err := checkExecWindow(c.ExecuteWindow)
	if err != nil {
		return false, common.CodePolicyExecuteWindowErr, err
	}
	c.ExecuteWindow = execWin

	src := &models.Source{}
	err = db.Model(src).Where("id =?", c.SrcID).First(src).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, common.CodeSourceNotExist, fmt.Errorf("models.Source(%v) not exist", c.SrcID)
		}
		return false, common.CodeServerErr, fmt.Errorf("query models.Source(%v) failed, %s", c.SrcID, err)
	}
	c.Bu = src.Bu

	// 仅归档需要校验目标端
	if c.Govern == common.GovernTypeArchive {
		dest := &models.Destination{}
		err = db.Model(dest).Where("id =?", c.DestID).First(dest).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return false, common.CodeDestNotExist, fmt.Errorf("models.Destination(%v) not exist", c.DestID)
			}
			return false, common.CodeServerErr, fmt.Errorf("query models.Destination(%v) failed, %s", c.DestID, err)
		}

		// 在创建策略时如果目标库、表都存在时需要提示用户自己确实是否要向目标表归档
		if !c.ForceArchive {
			destSvc := &DestService{}
			destSvc.ModelToService(dest)

			// 若目标端库名为空，则默认和源库保持一致
			if destSvc.DatabaseName == "" {
				destSvc.DatabaseName = src.DatabaseName
			}

			// 则默认和源库保持一致
			destSvc.TableName, err = common.GenerateDestTableName(src.TablesName, destSvc.TableName)
			if err != nil {
				return false, common.CodeTaskGenDestTabNameErr, fmt.Errorf("generate dest table name failed, %s", err)
			}

			// 检查目标端库是否存在
			_, code1, err := destSvc.DestDatabaseExist(ctx)
			if err != nil {
				return false, code1, fmt.Errorf("check models.Destination(%v) database exist failed, %s", c.DestID, err)
			} else if code1 != common.CodeDestDatabaseNotExist {
				return false, code1, fmt.Errorf("check models.Destination(%v) database exist", c.DestID)
			}

			// 检查目标段表是否存在
			_, code2, err := destSvc.DestTableExist(ctx)
			if err != nil {
				return false, code2, fmt.Errorf("check models.Destination(%v) table exist failed, %s", c.DestID, err)
			} else if code1 != common.CodeDestTableNotExist {
				return false, code1, fmt.Errorf("check models.Destination(%v) table exist", c.DestID)
			}
		}
	} else if c.Govern == common.GovernTypeDelete {
		c.Condition = utils.TrimmingSQLConditionEnding(c.Condition)
		if c.Condition == "" {
			return false, common.CodePolicyNeedConditions, fmt.Errorf("policy.Condition can not be empty")
		}
	}

	// 判断Condition条件是否正确
	clusterSvc, err := GetClusterServiceByClusterID(ctx, src.ClusterID)
	if err != nil {
		return false, common.CodeServerErr, err
	}

	tableNames := strings.Split(src.TablesName, ",")
	_, res, err := NewClusterDriver(clusterSvc).SQLExplain(ctx, src.DatabaseName, tableNames[0], c.ArchiveScope, c.Condition)
	if err != nil {
		return false, res, fmt.Errorf("check policy condition not pass, %s", err)
	}

	if c.Name == "" {
		c.Name = fmt.Sprintf("%s_%s_%s", c.Govern, src.Name, utils.RandStr(4))
	}

	return true, common.CodeOK, nil
}

func (c *PolicyService) CreatePolicy(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	policy := c.ServiceToModel()
	policy.Creator = u.UserName

	var count int64
	err := db.Model(policy).Where("name =?", policy.Name).Count(&count).Error
	if err != nil {
		log.Errorf("query models.Policy(name=%s) from db failed, %s", policy.Name, err)
		return common.CodeServerErr, err
	}
	if count != 0 {
		err = fmt.Errorf("models.Policy(name=%s) exist", policy.Name)
		return common.CodePolicyNameConflict, err
	}

	err = db.Save(policy).Error
	if err != nil {
		log.Errorf("save models.Policy(%+v) to db failed, %s", policy, err)
		return common.CodeServerErr, err
	}

	err = db.Model(policy).Where("name =?", policy.Name).First(policy).Error
	if err != nil {
		log.Errorf("query models.Policy(name=%s) from db failed, %s", policy.Name, err)
		return common.CodeServerErr, err
	}

	// 创建策略时通知PolicyHandle立即调度该策略
	handle := &PolicyQueueHandle{ID: policy.ID, HandleID: ctx.GetHeader(common.RequestID)}
	log.With(WithExtra[*PolicyQueueHandle](handle, ProcQueuing)...).Infof("policy %s push queue...", policy.Name)
	ok := PolicyQueue.Push(*handle)
	if ok {
		log.With(WithExtra[*PolicyQueueHandle](handle, ProcQueued)...).Infof("policy %s push queue sucess", policy.Name)
	} else {
		log.With(WithExtra[*PolicyQueueHandle](handle, ProcNotQueue)...).Infof("policy %s not push queue", policy.Name)
	}

	c.ModelToService(policy)

	return common.CodeOK, nil
}

func (c *PolicyService) CheckUpdateParameters(ctx *gin.Context) (bool, common.ServiceCode, error) {
	var err error
	log, db := common.ExtractContext(ctx)

	if common.InvalidUintID(c.ID) {
		return false, common.CodeInvalidID, fmt.Errorf("invalid Policy.id(%d)", c.ID)
	}

	if len(c.Name) == 0 || len(c.Name) >= 1024 {
		return false, common.CodePolicyNameLenErr, fmt.Errorf("validate name(%s) not pass", c.Name)
	}

	policy := &models.Policy{}
	err = db.Model(&policy).First(policy, "id = ?", c.ID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.Policy(id=%d) not exist", policy.ID)
			log.Error(err)
			return false, common.CodePolicyNotExist, err
		}
		err = fmt.Errorf("query models.Policy(id=%d) from db failed, %s", policy.ID, err)
		log.Error(err)
		return false, common.CodeServerErr, err
	}

	if c.Period != "" && !common.CheckPeriod(c.Period) {
		return false, common.CodePolicyPeriodErr, fmt.Errorf("validate period(%s) not pass", c.Period)
	} else if c.Period == "" {
		c.Period = policy.Period
	}

	// 当执行周期大于月度时需要检查day是否合理
	if c.Day != common.InvalidInt && !common.CheckPolicyDay(c.Period, c.Day) {
		return false, common.CodePolicyDayErr,
			fmt.Errorf("validate day(%v) not pass, day should in (1-31) when policy.Period greater than one month", c.Day)
	}

	if c.CleaningSpeed != "" && !common.CheckCleaningSpeed(c.CleaningSpeed) {
		return false, common.CodePolicyCleaningSpeedErr, fmt.Errorf("validate cleaning_speed(%s) not pass", c.CleaningSpeed)
	}

	if c.NotifyPolicy != "" && !common.CheckNotifyPolicyType(c.NotifyPolicy) {
		return false, common.CodePolicyNotifyPolicyErr, fmt.Errorf("validate notify_policy(%s) not pass", c.NotifyPolicy)
	}

	if len(c.ExecuteWindow) != 0 {
		_, execWin, err := checkExecWindow(c.ExecuteWindow)
		if err != nil {
			return false, common.CodePolicyExecuteWindowErr, err
		}
		c.ExecuteWindow = execWin
	}

	if !common.InvalidUintID(c.SrcID) && c.SrcID != policy.SrcID {
		err = fmt.Errorf("request.Policy.SrcID(%v) != models.Policy.SrcID(%v)", c.SrcID, policy.SrcID)
		log.Error(err)
		return false, common.CodePolicySrcIDImmutable, err
	}
	c.SrcID = policy.SrcID

	if c.Govern != "" && c.Govern != policy.Govern {
		err = fmt.Errorf("request.Policy.Govern(%v) != models.Policy.Govern(%v)", c.Govern, policy.Govern)
		log.Error(err)
		return false, common.CodePolicyGovernImmutable, err
	}

	// 只有归档需校验目标端ID
	if policy.Govern == common.GovernTypeArchive && !common.InvalidUintID(c.DestID) && c.DestID != policy.DestID {
		err = fmt.Errorf("request.Policy.DestID(%v) != models.Policy.DestID(%v)", c.DestID, policy.DestID)
		log.Error(err)
		return false, common.CodePolicyDestIDImmutable, err
	} else if c.Govern == common.GovernTypeDelete {
		c.Condition = utils.TrimmingSQLConditionEnding(c.Condition)
		if c.Condition == "" {
			return false, common.CodePolicyNeedConditions, fmt.Errorf("policy.Condition can not be empty")
		}
	}

	if c.Condition != policy.Condition {
		// 判断Condition条件是否正确
		src := &models.Source{}
		err = db.Model(src).Where("id =?", c.SrcID).First(src).Error
		if err != nil {
			return false, common.CodeServerErr, fmt.Errorf("query models.Source(%v) failed, %s", c.SrcID, err)
		}

		clusterSvc, err := GetClusterServiceByClusterID(ctx, src.ClusterID)
		if err != nil {
			return false, common.CodeServerErr, err
		}

		tableNames := strings.Split(src.TablesName, ",")
		_, res, err := NewClusterDriver(clusterSvc).SQLExplain(ctx, src.DatabaseName, tableNames[0], c.ArchiveScope, c.Condition)
		if err != nil {
			return false, res, fmt.Errorf("check policy condition not pass, %s", err)
		}
	}

	return true, common.CodeOK, nil
}

func (c *PolicyService) UpdatePolicy(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)

	policy := &models.Policy{}
	err := db.Model(&policy).First(policy, "id = ?", c.ID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = fmt.Errorf("query models.Policy(id=%d) not exist", policy.ID)
			log.Error(err)
			return common.CodeDestNotExist, err
		}
		err = fmt.Errorf("query models.Policy(id=%d) from db failed, %s", policy.ID, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	var count int64
	err = db.Model(policy).Where("name =? AND id !=?", c.Name, c.ID).Count(&count).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Errorf("query models.Policy(name =%v AND id !=%v) from db failed, %s", c.Name, c.ID, err)
		return common.CodeServerErr, err
	}
	if count != 0 {
		err = fmt.Errorf("models.Policy(name=%s) exist", c.Name)
		log.Error(err)
		return common.CodePolicyNameConflict, err
	}

	newPolicy := *policy
	newPolicy.Name = c.Name
	newPolicy.Description = c.Description
	newPolicy.Editor = u.UserName
	newPolicy.Enable = c.Enable
	newPolicy.Pause = c.Pause
	newPolicy.RebuildFlag = c.RebuildFlag
	newPolicy.RetainSrcData = c.RetainSrcData
	newPolicy.Period = utils.Ternary[common.PeriodType](c.Period == "", policy.Period, c.Period)
	newPolicy.Day = utils.Ternary[int](c.Day == common.InvalidInt, policy.Day, c.Day)
	newPolicy.CleaningSpeed = utils.Ternary[common.CleaningSpeedType](c.CleaningSpeed == "", policy.CleaningSpeed, c.CleaningSpeed)
	newPolicy.Condition = utils.Ternary[string](c.Condition == "", policy.Condition, c.Condition)
	newPolicy.NotifyPolicy = utils.Ternary[common.NotifyPolicyType](c.NotifyPolicy == "", policy.NotifyPolicy, c.NotifyPolicy)
	if len(c.ExecuteWindow) != 0 {
		newPolicy.ExecuteWindow, _ = json.Marshal(c.ExecuteWindow)
	}
	if len(c.Relevant) != 0 {
		newPolicy.Relevant, _ = json.Marshal(c.Relevant)
	}

	err = db.Transaction(func(db *gorm.DB) error {
		err = db.Save(&newPolicy).Error
		if err != nil {
			log.Errorf("update models.Policy(%+v) from db failed, %s", policy, err)
			return err
		}

		err = db.Model(&models.Task{}).
			Where("policy_id =? AND task_status IN(?)", newPolicy.ID, common.TaskStatusCanUpdate).
			Update("need_check_execute_date", true).Error
		if err != nil {
			log.Errorf("update models.Task(policy_id=%v AND task_status IN(%v)) need_check_execute_date=true failed, %s", newPolicy.ID, common.TaskStatusCanUpdate, err)
			return err
		}

		_, err = new(PolicyRevisionService).CreatePolicyRevision(ctx, policy, &newPolicy)
		if err != nil {
			log.Errorf("save models.PolicyRevision to db failed, %s", err)
			return err
		}
		return nil
	})
	if err != nil {
		return common.CodeServerErr, err
	}

	// 策略发生变化时通知PolicyHandle立即调度该策略
	handle := &PolicyQueueHandle{ID: policy.ID, HandleID: ctx.GetHeader(common.RequestID)}
	log.With(WithExtra[*PolicyQueueHandle](handle, ProcQueuing)...).Infof("policy %s push queue...", policy.Name)
	ok := PolicyQueue.Push(*handle)
	if ok {
		log.With(WithExtra[*PolicyQueueHandle](handle, ProcQueued)...).Infof("policy %s push queue sucess", policy.Name)
	} else {
		log.With(WithExtra[*PolicyQueueHandle](handle, ProcNotQueue)...).Infof("policy %s not push queue", policy.Name)
	}

	c.ModelToService(&newPolicy)

	return common.CodeOK, nil
}

func (c *PolicyService) QueryPolicy(ctx *gin.Context, queryMap map[string]string) (any, common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)

	enable, enableOk := ctx.GetQuery("enable")
	pause, pauseOk := ctx.GetQuery("pause")
	rebuildFlag, rebuildFlagOk := ctx.GetQuery("rebuild_flag")
	retainSrc, retainSrcOk := ctx.GetQuery("retain_src_data")

	res, err := common.NewPageList[[]models.Policy](db).
		QueryPaging(ctx).
		Order("id desc").
		Query(
			common.FilterFuzzyStringMap(queryMap),
			common.FilterID(c.ID),
			common.FilterCustomUintID("src_id", c.SrcID),
			common.FilterCustomUintID("dest_id", c.DestID),
			common.FilterCustomBool("enable", enable, enableOk),
			common.FilterCustomBool("pause", pause, pauseOk),
			common.FilterCustomBool("rebuild_flag", rebuildFlag, rebuildFlagOk),
			common.FilterCustomBool("retain_src_data", retainSrc, retainSrcOk),
		)
	if err != nil {
		err = fmt.Errorf("query models.Policy from db faield, %s", err)
		log.Error(err)
		return nil, common.CodeServerErr, err
	}

	ids := make([]uint, 0, len(res.Items))
	for _, item := range res.Items {
		ids = append(ids, item.SrcID)
	}

	var sourceMap = make(map[uint]*models.Source, len(res.Items))
	var sources []models.Source
	err = db.Model(models.Source{}).
		Select("id, name, cluster_name, cluster_id, database_name, tables_name").
		Where("id IN (?)", ids).
		Find(&sources).Error
	if err != nil {
		log.Errorf("query models.Source failed, %s", err)
	} else {
		for i, v := range sources {
			sourceMap[v.ID] = &sources[i]
		}
	}

	ret := common.NewPageList[[]PolicyService](db)
	ret.Page = res.Page
	ret.PageSize = res.PageSize
	ret.Total = res.Total
	for i, v := range res.Items {
		p := &PolicyService{}
		p.ModelToService(&res.Items[i])

		if s, ok := sourceMap[v.SrcID]; ok {
			p.SrcName = s.Name
			p.SrcClusterName = s.ClusterName
			p.SrcClusterID = s.ClusterID
			p.SrcDatabaseName = s.DatabaseName
			p.SrcTablesName = s.TablesName
		}

		ret.Items = append(ret.Items, *p)
	}

	return ret, common.CodeOK, nil
}

func (c *PolicyService) DeletePolicy(ctx *gin.Context) (common.ServiceCode, error) {
	log, db := common.ExtractContext(ctx)
	u := common.ExtractUserInfo(ctx)
	var err error

	policy := &models.Policy{}

	// 若ID有效则以ID为准, 否则以name为准
	if !common.InvalidUintID(c.ID) {
		err = db.Model(policy).First(policy, "id = ?", c.ID).Error
	} else {
		err = db.Model(policy).First(policy, "name = ?", c.Name).Error
	}
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Infof("query models.Policy(%v|%v) not exist", c.ID, c.Name)
			return common.CodePolicyNotExist, err
		}
		err = fmt.Errorf("query models.Policy(%v|%v) from db failed, %s", c.ID, c.Name, err)
		log.Error(err)
		return common.CodeServerErr, err
	}

	// 未完成的任务会使用到，不可删除
	var task []models.Task
	err = db.Model(&models.Task{}).Select("name").Where("policy_id =? AND task_status NOT IN (?)", c.ID, common.TaskStatusHasFinished).Find(&task).Error
	if err != nil {
		err = fmt.Errorf("query models.Task(policy_id=%v AND task_status IN (%v)) from db failed, %s", c.ID, common.TaskStatusHasFinished, err)
		log.Error(err)
		return common.CodeServerErr, err
	}
	if len(task) != 0 {
		err = fmt.Errorf("policy(%s) has been used form Task(%s)", policy.Name, task[0].Name)
		log.Error(err)
		return common.CodePolicyUsingTask, err
	}

	err = db.Transaction(func(db *gorm.DB) error {
		// 更新任务
		db.Model(&models.Policy{}).Where("id =?", policy.ID).Update("editor", u.UserName)
		err = db.Delete(&models.Policy{}, "id =?", policy.ID).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("delete models.Policy(%v) from db failed, %s", policy.ID, err)
		}

		// 删除任务修订记录
		err = db.Delete(&models.PolicyRevision{}, "policy_id =?", policy.ID).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("delete models.PolicyRevision(%v) from db failed, %s", policy.ID, err)
		}
		return nil
	})
	if err != nil {
		log.Errorf("delete policy(%v) failed, %s", policy.ID, err)
	}

	return common.CodeOK, nil
}

func checkExecWindow(execWin []string) (bool, []string, error) {
	switch len(execWin) {
	case 0:
		execWin = []string{"00:00:00", "23:59:59"}
	case 2:
		s, err1 := time.Parse(time.TimeOnly, execWin[0])
		e, err2 := time.Parse(time.TimeOnly, execWin[1])
		if err1 != nil || err2 != nil {
			return false, nil,
				fmt.Errorf("validate execute_window(%v) not pass, it should look like: [] or [\"03:00:00\", \"05:00:00\"]", execWin)
		}
		execWin = []string{s.Format(time.TimeOnly), e.Format(time.TimeOnly)}
	default:
		return false, nil, fmt.Errorf("validate execute_window(%v) not pass, it should look like: [] or [\"03:00:00\", \"05:00:00\"]", execWin)
	}

	return true, execWin, nil
}

func (c *PolicyService) ServiceToModel() *models.Policy {
	m := &models.Policy{}
	m.ID = c.ID
	m.Creator = c.Creator
	m.Editor = c.Editor
	m.CreatedAt, _ = time.ParseInLocation(time.DateTime, c.CreatedAt, time.Now().Location())
	m.UpdatedAt, _ = time.ParseInLocation(time.DateTime, c.UpdatedAt, time.Now().Location())
	m.Name = c.Name
	m.Description = c.Description
	m.Enable = c.Enable
	m.Period = c.Period
	m.Day = c.Day
	m.Bu = c.Bu
	m.Pause = c.Pause
	m.RebuildFlag = c.RebuildFlag
	m.CleaningSpeed = c.CleaningSpeed
	m.SrcID = c.SrcID
	m.Govern = c.Govern
	m.Condition = c.Condition
	m.ArchiveScope = c.ArchiveScope
	m.RetainSrcData = c.RetainSrcData
	m.DestID = c.DestID
	m.NotifyPolicy = c.NotifyPolicy
	m.ExecuteWindow, _ = json.Marshal(c.ExecuteWindow)
	m.Relevant, _ = json.Marshal(c.Relevant)
	return m
}

func (c *PolicyService) ModelToService(m *models.Policy) *PolicyService {
	c.ID = m.ID
	c.Creator = m.Creator
	c.Editor = m.Editor
	c.CreatedAt = m.CreatedAt.Format(time.DateTime)
	c.UpdatedAt = m.UpdatedAt.Format(time.DateTime)
	c.Name = m.Name
	c.Description = m.Description
	c.Bu = m.Bu
	c.Enable = m.Enable
	c.Period = m.Period
	c.Day = m.Day
	c.Pause = m.Pause
	c.RebuildFlag = m.RebuildFlag
	c.CleaningSpeed = m.CleaningSpeed
	c.SrcID = m.SrcID
	c.Govern = m.Govern
	c.Condition = m.Condition
	c.ArchiveScope = m.ArchiveScope
	c.RetainSrcData = m.RetainSrcData
	c.DestID = m.DestID
	c.NotifyPolicy = m.NotifyPolicy
	_ = json.Unmarshal(m.ExecuteWindow, &c.ExecuteWindow)
	_ = json.Unmarshal(m.Relevant, &c.Relevant)
	return c
}
