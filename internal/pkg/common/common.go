package common

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Context struct {
	Context context.Context
	Wg      *sync.WaitGroup
	Cancel  context.CancelFunc
	Log     *zap.SugaredLogger
	DB      *gorm.DB
	data    map[string]any
}

func NewContext() *Context {
	return &Context{
		Wg: &sync.WaitGroup{},
	}
}

func (c *Context) WithWaitGroup(wg *sync.WaitGroup) *Context {
	c.Wg = wg
	return c
}

func (c *Context) WithContext(ctx context.Context) *Context {
	c.Context = ctx
	return c
}

func (c *Context) WithCancel(cancel context.CancelFunc) *Context {
	c.Cancel = cancel
	return c
}
func (c *Context) WithLog(log *zap.SugaredLogger) *Context {
	c.Log = log
	return c
}
func (c *Context) WithDB(db *gorm.DB) *Context {
	c.DB = db
	return c
}

func (c *Context) SetData(key string, value any) *Context {
	if c.data == nil {
		c.data = make(map[string]any)
	}
	c.data[key] = value
	return c
}

func (c *Context) GetData(key string) (any, bool) {
	v, ok := c.data[key]
	return v, ok
}

type Response struct {
	ServiceCode
	Error string `json:"error,omitempty"` // 错误信息
	Data  any    `json:"data"`            // 返回数据
}

type ServiceCode struct {
	Code    int    `json:"code"`    // 返回码
	Message string `json:"message"` // 返回信息
}

func ServiceCode2HttpCode(r ServiceCode) int {
	if r.Code == 0 {
		return http.StatusOK
	}
	return r.Code / 10000
}

func InvalidUintID(id uint) bool {
	return id == InvalidUint
}

func InvalidIntID(id int) bool {
	return id == InvalidInt
}

func ParsingQueryUintID(id string) uint {
	if i, err := strconv.Atoi(id); err == nil {
		return uint(i)
	}
	return InvalidUint
}

func CheckPeriod(s PeriodType) bool {
	switch s {
	case PeriodOnce, PeriodMonthly, PeriodQuarterly, PeriodSixMonths, PeriodYearly,
		PeriodDay, PeriodTwoDay, PeriodWeekly, PeriodTwoWeeks:
		return true
	default:
		return false
	}
}

func CheckPolicyDay(p PeriodType, day int) bool {
	switch p {
	case PeriodOnce, PeriodDay, PeriodTwoDay, PeriodWeekly, PeriodTwoWeeks:
		// 执行周期小于月度的day不影响
		return true
	case PeriodMonthly, PeriodQuarterly, PeriodSixMonths, PeriodYearly:
		if day >= 1 && day <= 31 {
			return true
		}
	default:
		return false
	}
	return false
}

func CheckCleaningSpeed(s CleaningSpeedType) bool {
	switch s {
	case CleaningSpeedSteady, CleaningSpeedBalanced, CleaningSpeed:
		return true
	default:
		return false
	}
}

func CheckGovernType(s GovernType) bool {
	switch s {
	case GovernTypeTruncate, GovernTypeDelete, GovernTypeBackupDelete, GovernTypeArchive, GovernTypeRebuild:
		return true
	default:
		return false
	}
}

func CheckNotifyPolicyType(s NotifyPolicyType) bool {
	switch s {
	case NotifyPolicyTypeSilence, NotifyPolicyTypeSuccess, NotifyPolicyTypeFailed, NotifyPolicyTypeAlways:
		return true
	default:
		return false
	}
}

func CheckTaskStatusType(s TaskStatusType) bool {
	switch s {
	case TaskStatusScheduled, TaskStatusSupplementFailed, TaskStatusWaiting, TaskStatusExecCheckFailed,
		TaskStatusExecuting, TaskStatusSuccess, TaskStatusExecFailed, TaskStatusTimeout:
		return true
	default:
		return false
	}
}

func CheckTaskConflictLevel(lev TaskConflictLevelType) bool {
	switch lev {
	case TaskConflictLevelCluster, TaskConflictLevelDatabase, TaskConflictLevelTable:
		return true
	default:
		return false
	}
}

// CheckSameShardingTables 检查给定的表名是否属于同一个分库分表
func CheckSameShardingTables(name string) (bool, string, error) {
	tableNames := strings.Split(name, ",")
	if len(tableNames) == 0 {
		return false, "", fmt.Errorf("table name empty")
	}
	if len(tableNames) == 1 {
		return false, tableNames[0], nil
	}

	// 提取分表原始名字
	baseName := ""
	match := regexp.MustCompile(`^([^_]+(?:_[^_]+)*)_`).FindStringSubmatch(tableNames[0])
	if len(match) > 1 {
		baseName = match[1]
	} else {
		return false, "", fmt.Errorf("unable to retrieve the original table name(%s) for the sharded table", tableNames[0])
	}

	re := regexp.MustCompile(`^` + regexp.QuoteMeta(baseName) + `_\d+$`)
	for _, tableName := range tableNames {
		if !re.MatchString(tableName) {
			return false, baseName, fmt.Errorf("table(%s) does not conform to the sharding table naming rules", tableName)
		}
	}

	return true, baseName, nil
}

func GenerateDestTableName(srcTabName, destTabName string) (string, error) {
	// 判断源表是不是分表，及分表的原始表名
	_, baseName, err := CheckSameShardingTables(srcTabName)
	if err != nil {
		return "", fmt.Errorf("check same sharding tables failed, %s", err)
	}

	// 实例化目标端表名
	data := time.Now().Format("2006-01")
	destTabName = strings.ReplaceAll(destTabName, "{source_table}", baseName)
	destTabName = strings.ReplaceAll(destTabName, "{YYYY-MM}", data)
	return destTabName, nil
}

// JudgeTaskCouldCheckBeforeExec 提前一天检查任务是否满足执行条件，以便于人可以提前介入修复异常
func JudgeTaskCouldCheckBeforeExec(execDate string) bool {
	if time.Now().Add(time.Hour*24).Format(time.DateOnly) >= execDate {
		return true
	}
	return false
}

func ExcludeTablesFilter(database string, tables, excludeTables []string) []string {
	splitTable := func(name string) (string, string) {
		ss := strings.Split(name, ".")
		switch len(ss) {
		case 1:
			return "", ss[0]
		case 2:
			return ss[0], ss[1]
		default:
			return ss[0], ss[1]
		}
	}

	newTables := make([]string, 0, len(tables))
	for _, table := range tables {
		skip := false
		for _, s := range excludeTables {
			d, t := splitTable(s)
			if d == "" && t == table {
				skip = true
				break
			}

			if d != "" && d == database && t == table {
				skip = true
				break
			}
		}

		if !skip {
			newTables = append(newTables, table)
		}
	}
	return newTables
}
