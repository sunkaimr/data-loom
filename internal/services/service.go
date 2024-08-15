package services

type Model struct {
	ID        uint   `json:"id"`                   // ID
	CreatedAt string `json:"created_at"`           // 创建时间
	UpdatedAt string `json:"updated_at,omitempty"` // 修改时间
	Creator   string `json:"creator"`              // 创建人
	Editor    string `json:"editor"`               // 修改人
}
