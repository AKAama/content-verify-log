package model

import (
	"database/sql/driver"
	"encoding/json"
	"time"

	"gorm.io/gorm"
)

// VerifyContent 表示 tbl_verify_content 表
type VerifyContent struct {
	ID        uint           `gorm:"primarykey" json:"id"`
	TaskID    string         `gorm:"column:taskId" json:"task_id"` // taskId 字段
	Content   JSONContent    `gorm:"type:text" json:"content"`     // JSON 内容字段
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"deleted_at,omitempty"`
}

// TableName 指定表名
func (VerifyContent) TableName() string {
	return "tbl_verify_content"
}

// JSONContent 是一个自定义类型，用于存储和解析 JSON 内容
type JSONContent struct {
	Data map[string]interface{} `json:"-"`
	Raw  string                 `json:"-"`
}

// Value 实现 driver.Valuer 接口，用于将 JSONContent 存储到数据库
func (j JSONContent) Value() (driver.Value, error) {
	if j.Raw != "" {
		return j.Raw, nil
	}
	if j.Data != nil {
		bytes, err := json.Marshal(j.Data)
		if err != nil {
			return nil, err
		}
		return string(bytes), nil
	}
	return nil, nil
}

// Scan 实现 sql.Scanner 接口，用于从数据库读取 JSONContent
func (j *JSONContent) Scan(value interface{}) error {
	if value == nil {
		j.Data = nil
		j.Raw = ""
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return nil
	}

	j.Raw = string(bytes)

	// 尝试解析 JSON
	var data map[string]interface{}
	if err := json.Unmarshal(bytes, &data); err != nil {
		// 如果解析失败，保留原始字符串
		j.Data = nil
		return nil
	}
	j.Data = data
	return nil
}

// UnmarshalJSON 实现 json.Unmarshaler 接口
func (j *JSONContent) UnmarshalJSON(data []byte) error {
	j.Raw = string(data)
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	j.Data = m
	return nil
}

// MarshalJSON 实现 json.Marshaler 接口
func (j JSONContent) MarshalJSON() ([]byte, error) {
	if j.Data != nil {
		return json.Marshal(j.Data)
	}
	if j.Raw != "" {
		return []byte(j.Raw), nil
	}
	return []byte("{}"), nil
}

// GetParsedContent 返回解析后的 JSON 内容
func (j *JSONContent) GetParsedContent() map[string]interface{} {
	return j.Data
}

// GetRawContent 返回原始的 JSON 字符串
func (j *JSONContent) GetRawContent() string {
	return j.Raw
}
