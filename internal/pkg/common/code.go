package common

// 通用错误码
var (
	CodeOK               = ServiceCode{0, ""}
	CodeBindErr          = ServiceCode{4000000, "参数解析错误"}
	CodeInvalidID        = ServiceCode{4000001, "缺少ID或ID无效"}
	CodeMissAuth         = ServiceCode{4010001, "No Authorization"}
	CodeTokenErr         = ServiceCode{4010002, "token格式错误"}
	CodeTokenExpired     = ServiceCode{4010003, "token已过期"}
	CodeDenied           = ServiceCode{4030000, "权限不足"}
	CodeAdminOnly        = ServiceCode{4030001, "权限不足，需要管理员权限"}
	CodeNotFound         = ServiceCode{4040000, "not found"}
	CodeServerErr        = ServiceCode{5000000, "服务器内部错误"}
	CodeDecryptPasswdErr = ServiceCode{5000001, "服务器内部错误，提取密码失败"}
	CodeEncryptPasswdErr = ServiceCode{5000002, "服务器内部错误，加密密码失败"}
	CodeConfigErr        = ServiceCode{5000003, "服务器内部错误，获取系统配置失败"}
)

// 用户模块错误码范围: 4xx1xx - 5xx1xx
var (
	CodeUserParamErr    = ServiceCode{4000101, "用户名或密码为空"}
	CodeUserDeniedLdap  = ServiceCode{4000102, "不可修改LDAP用户"}
	CodeUserPasswdErr   = ServiceCode{4000103, "用户名或密码错误"}
	CodeUserDelErr      = ServiceCode{4000104, "用户删除失败"}
	CodeUserAdminDelErr = ServiceCode{4000105, "管理员用户不可删除"}
	CodeUserNotExist    = ServiceCode{4040101, "用户不存在"}
	CodeUserExisted     = ServiceCode{4090101, "用户已存在"}
)

// 源端信息模块错误码范围: 4xx2xx - 5xx2xx
var (
	CodeSourceExist              = ServiceCode{2000201, "源端信息已存在"}
	CodeSourceNameErr            = ServiceCode{4000201, "源端名字不合法"}
	CodeSourceClusterIDNull      = ServiceCode{4000202, "集群ID不能为空"}
	CodeSourceDatabaseNull       = ServiceCode{4000203, "源端库不能为空"}
	CodeSourceTableNameNull      = ServiceCode{4000204, "源端表不能为空"}
	CodeSourceParamErr           = ServiceCode{4000205, "源端信息校验不通过"}
	CodeSourceDatabaseNotExist   = ServiceCode{4000206, "源端信息校验不通过，源端库不存在"}
	CodeSourceTableNotExist      = ServiceCode{4000207, "源端信息校验不通过，源端表不存在"}
	CodeSourceUsing              = ServiceCode{4000208, "源端已被策略使用，请先删除对应策略"}
	CodeSourceTableHasPrimaryKey = ServiceCode{4000209, "源表必须包含主键"}
	CodeSourceTableNameErr       = ServiceCode{4000210, "源端包含多张表时必须为同一个分表"}
	CodeSourceNotExist           = ServiceCode{4040201, "源端信息不存在"}
	CodeSourceNameConflict       = ServiceCode{4090201, "源端名字已存在"}
	CodeSourceQueryDatabaseErr   = ServiceCode{5000201, "查询库列表失败，请联系管理员处理"}
	CodeSourceQueryTableErr      = ServiceCode{5000202, "查询表列表失败，请联系管理员处理"}
	CodeSourceQueryTableSizeErr  = ServiceCode{5000203, "查询表大小失败，请联系管理员处理"}
)

// 归档库连接信息模块错误码范围: 4xx3xx - 5xx3xx
var (
	CodeConnTestOK           = ServiceCode{2000301, "连接信息测试通过"}
	CodeConnNameEmpty        = ServiceCode{4000302, "连接信息名字为空"}
	CodeConnNameConflict     = ServiceCode{4000303, "连接信息名字已存在"}
	CodeConnBuNameEmpty      = ServiceCode{4000304, "连接信息BU为空"}
	CodeConnStorageErr       = ServiceCode{4000305, "连接信息存储介质不合法"}
	CodeConnTestErr          = ServiceCode{4000306, "测试归档连接信息不通过"}
	CodeConnParamErr         = ServiceCode{4000307, "连接信息校验不通过"}
	CodeConnUsingDest        = ServiceCode{4000308, "连接信息已被使用，请先删除对应目标端信息"}
	CodeConnUsingTask        = ServiceCode{4000309, "连接信息已被使用，请先删除对应任务"}
	CodeConnConnectMysql     = ServiceCode{4000310, "连接数据库失败，请联系管理员处理"}
	CodeConnStorageImmutable = ServiceCode{4000311, "归档库存储介质类型不可修改"}
	CodeConnNotExist         = ServiceCode{4040301, "连接信息不存在"}
	CodeConnExist            = ServiceCode{4090301, "连接信息已存在"}
)

// 归档库连接信息模块错误码范围: 4xx4xx - 5xx4xx
var (
	CodeDestNameEmpty             = ServiceCode{4000401, "目标端信息名字为空"}
	CodeDestNameLenErr            = ServiceCode{4000402, "目标端信息名字不合法"}
	CodeDestStorageNotMatch       = ServiceCode{4000403, "目标端存储介质和连接信息存储介质不一致"}
	CodeDestStorageImmutable      = ServiceCode{4000404, "目标端存储介质类型不可修改"}
	CodeDestConnectionIDImmutable = ServiceCode{4000405, "目标端连接信息不可修改"}
	CodeDestDBImmutable           = ServiceCode{4000406, "目标端归档库名不可修改"}
	CodeDestTableImmutable        = ServiceCode{4000407, "目标端归档表名不可修改"}
	CodeDestCompressImmutable     = ServiceCode{4000408, "目标端压缩存储不可修改"}
	CodeDestUsingPolicy           = ServiceCode{4000409, "目标端已被策略使用，请先删除对应策略"}
	CodeDestUsingTask             = ServiceCode{4000410, "目标端已被任务使用，请先删除对应任务"}
	CodeDestTableNameErr          = ServiceCode{4000410, "目标端归档表名不合法"}
	CodeDestCompressOnlyMysql     = ServiceCode{4000408, "仅MySQL归档介质可设置压缩存储"}
	CodeDestNotExist              = ServiceCode{4040401, "目标端不存在"}
	CodeDestDatabaseNotExist      = ServiceCode{4040402, "目标端库不存在"}
	CodeDestTableNotExist         = ServiceCode{4040403, "目标端表不存在"}
	CodeDestNameConflict          = ServiceCode{4090401, "目标端信息名字已存在"}
	CodeDestExist                 = ServiceCode{4090402, "目标端已存在"}
	CodeDestDatabaseExist         = ServiceCode{4090403, "目标端库已存在"}
	CodeDestTablesExist           = ServiceCode{4090404, "目标端表已存在"}
	CodeDestQueryDatabaseErr      = ServiceCode{5000401, "查询目标端库列表失败，请联系管理员处理"}
	CodeDestQueryTableErr         = ServiceCode{5000402, "查询目标端表列表失败，请联系管理员处理"}
)

// 策略模块错误码范围: 4xx5xx - 5xx5xx
var (
	CodePolicyNameLenErr       = ServiceCode{4000502, "策略名字不合法"}
	CodePolicyPeriodErr        = ServiceCode{4000503, "执行周期不合法"}
	CodePolicyGovernErr        = ServiceCode{4000504, "数据治理方式不合法"}
	CodePolicyCleaningSpeedErr = ServiceCode{4000505, "清理速度不合法"}
	CodePolicyNotifyPolicyErr  = ServiceCode{4000506, "通知策略不合法"}
	CodePolicyExecuteWindowErr = ServiceCode{4000507, "执行时间窗口参数格式错误"}
	CodePolicySrcIDImmutable   = ServiceCode{4000508, "策略的源端不可修改"}
	CodePolicyDestIDImmutable  = ServiceCode{4000509, "策略的目标端不可修改"}
	CodePolicyGovernImmutable  = ServiceCode{4000510, "策略的数据治理方式不可修改"}
	CodePolicyConditionsErr    = ServiceCode{4000511, "策略的治理条件检查不通过"}
	CodePolicyUsingTask        = ServiceCode{4000512, "策略已被使用，请先删除对应任务"}
	CodePolicyDayErr           = ServiceCode{4000513, "策略期望执行日不合法"}
	CodePolicyNeedConditions   = ServiceCode{4000511, "删除数据时必须指定删除条件"}
	CodePolicyNotExist         = ServiceCode{4040501, "策略不存在"}
	CodePolicyNameConflict     = ServiceCode{4090501, "策略名字已存在"}
)

// 任务模块错误码范围: 4xx6xx - 5xx6xx
var (
	CodeTaskNameLenErr          = ServiceCode{4000601, "任务名字不合法"}
	CodeTaskExecDateErr         = ServiceCode{4000602, "任务执行日期不合法"}
	CodeTaskNotifyPolicyErr     = ServiceCode{4000603, "通知策略不合法"}
	CodeTaskStatusImmutable     = ServiceCode{4000604, "当前状态的任务不可修改"}
	CodeTaskStatusNoDelete      = ServiceCode{4000605, "当前状态的任务不可删除"}
	CodeTaskNotReachedExecWin   = ServiceCode{4000607, "未在执行时间窗口"}
	CodeTaskMissedExecWin       = ServiceCode{4000608, "错过本次执行时间窗口，需等待下个执行窗口。"}
	CodeTaskSrcClusterConflict  = ServiceCode{4000609, "同一集群执行中的任务达到上限"}
	CodeTaskSrcDatabaseConflict = ServiceCode{4000609, "同一库执行中的任务达到上限"}
	CodeTaskSrcTableConflict    = ServiceCode{4000609, "同一表执行中的任务达到上限"}
	CodeTaskStatusErr           = ServiceCode{4000610, "任务状态不合法"}
	CodeTaskExecDateNotReached  = ServiceCode{4000611, "任务未到执行日期"}
	CodeTaskStatusUpdateDenied  = ServiceCode{4030601, "权限不足无法更新任务结果"}
	CodeTaskNotExist            = ServiceCode{4040601, "任务不存在"}
	CodeTaskGenDestTabNameErr   = ServiceCode{5000601, "生成归档库表名失败"}
	CodeTaskParallelUpperLimit  = ServiceCode{5000602, "达到任务并发上限"}
)

// 工作流模块错误码范围: 4xx7xx - 5xx7xx
var (
	CodeWorkFlowPending       = ServiceCode{5000701, "工作流Pending"}
	CodeWorkFlowGetStatusErr  = ServiceCode{5000702, "获取工作流状态失败"}
	CodeWorkFlowCallFailed    = ServiceCode{5000703, "调用工作流失败"}
	CodeWorkFlowUnknownStatus = ServiceCode{5000704, "工作流状态未知"}
	CodeWorkFlowUnsupported   = ServiceCode{5000705, "未找到匹配的工作流"}
)

// 集群管理模块错误码范围: 4xx8xx - 5xx8xx
var (
	CodeClusterCollectedTaskRunning   = ServiceCode{2000801, "收集大表任务开始运行，可能需要几分钟时间"}
	CodeClusterNameErr                = ServiceCode{4000801, "集群名字不合法"}
	CodeClusterIDErr                  = ServiceCode{4000802, "集群ID不合法"}
	CodeClusterNameAndIDErr           = ServiceCode{4000803, "集群名字和ID不能同时为空"}
	CodeClusterIDEmpty                = ServiceCode{4000804, "集群ID为空"}
	CodeClusterDatabaseEmpty          = ServiceCode{4000805, "库名称为空"}
	CodeClusterServiceAddrErr         = ServiceCode{4000806, "集群服务地址不合法"}
	CodeClusterUnsupportedClusterType = ServiceCode{4000807, "暂不支持的集群类型"}
	CodeClusterUnreachable            = ServiceCode{4000808, "无法连接集群，地址、用户名或密码错误"}
	CodeClusterSyncImmutable          = ServiceCode{4000809, "从外部同步的集群信息不支持修改"}
	CodeClusterUsing                  = ServiceCode{4000810, "集群已被源端使用，请先删除对应源端"}
	CodeClusterCollectedTaskExisted   = ServiceCode{4000811, "已有收集大表任务正在运行，稍后再试"}
	CodeClusterNotExist               = ServiceCode{4040801, "集群不存在"}
	CodeClusterExisted                = ServiceCode{4090801, "集群名字或ID已存在"}
	CodeClusterFreeDiskErr            = ServiceCode{5000801, "获取源端剩余磁盘空间失败，请联系管理员处理"}
	CodeClusterExtractingErr          = ServiceCode{5000802, "提取集群信息失败"}
)

// 配置模块错误码范围: 4xx9xx - 5xx9xx
var (
	CodeConfigNoticeOK         = ServiceCode{2000001, "通知发送成功请注意查收"}
	CodeConfigConflictLevelErr = ServiceCode{4000901, "任务冲突级别配置不合法"}
	CodeConfigNoticeUserErr    = ServiceCode{4000902, "通知测试用户不能为空"}
	CodeConfigNoticeErr        = ServiceCode{4000903, "通知测试失败"}
)
