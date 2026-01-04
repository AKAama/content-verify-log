package model

// ProcessedContent 表示处理后的内容，存储到 DuckDB
type ProcessedContent struct {
	ID           string `json:"id"`            // UUID
	OriginalText string `json:"original_text"` // 原文（对应 checkresultstr）
	ModifiedText string `json:"modified_text"` // 修改后的文章
	PID          uint   `json:"pid"`           // 对应 tbl_verify_content 表的 taskId
}

// TableName 指定表名
func (ProcessedContent) TableName() string {
	return "processed_content"
}
