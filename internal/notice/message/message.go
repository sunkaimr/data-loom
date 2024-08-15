package message

type Message struct {
	TaskID             uint
	TaskName           string
	SrcClusterName     string
	SrcDatabaseName    string
	SrcTablesName      string
	Govern             string
	Condition          string
	TaskStartTime      string
	TaskEndTime        string
	TaskDuration       string
	TaskStatus         string
	TaskResultQuantity int
	TaskResultSize     int
	TaskReason         string
	Relevant           []string

	HomeURL         string
	TaskURL         string
	TaskStatusColor string // 失败的提示颜色：#F33， 成功的颜色：008000
}
