package service

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"content-verify-log/pkg/model"

	"go.uber.org/zap"
)

type ContentProcessor struct{}

func NewContentProcessor() *ContentProcessor {
	return &ContentProcessor{}
}

// ProcessContent 处理验证内容，提取并处理 JSON 数据
func (p *ContentProcessor) ProcessContent(verifyContent *model.VerifyContent) (*model.ProcessedContent, error) {
	if verifyContent == nil {
		return nil, fmt.Errorf("验证内容为空")
	}

	// 解析 JSON
	jsonData := verifyContent.Content.GetParsedContent()
	if jsonData == nil {
		// 尝试重新解析
		raw := verifyContent.Content.GetRawContent()
		if raw == "" {
			return nil, fmt.Errorf("内容为空")
		}
		if err := json.Unmarshal([]byte(raw), &jsonData); err != nil {
			return nil, fmt.Errorf("JSON 解析失败: %v", err)
		}
	}

	// 检查是否有 data 字段包装（根据实际 JSON 结构）
	var dataObj map[string]interface{}
	if data, exists := jsonData["data"]; exists {
		if dataMap, ok := data.(map[string]interface{}); ok {
			dataObj = dataMap
		} else {
			dataObj = jsonData
		}
	} else {
		dataObj = jsonData
	}

	// 提取 checkresultstr（原文）
	originalText, ok := dataObj["checkresultstr"].(string)
	if !ok {
		// 尝试其他可能的字段名
		if val, exists := dataObj["checkResultStr"]; exists {
			originalText, _ = val.(string)
		} else if val, exists := dataObj["check_result_str"]; exists {
			originalText, _ = val.(string)
		}
		if originalText == "" {
			return nil, fmt.Errorf("未找到原文字段 checkresultstr")
		}
	}

	// 提取 checkresultjson（错误信息）
	checkResultJSON, ok := dataObj["checkresultjson"]
	if !ok {
		// 尝试其他可能的字段名
		if val, exists := dataObj["checkResultJson"]; exists {
			checkResultJSON = val
		} else if val, exists := dataObj["check_result_json"]; exists {
			checkResultJSON = val
		}
	}

	// 处理修改后的文章
	modifiedText, err := p.applyCorrections(originalText, checkResultJSON)
	if err != nil {
		zap.S().Warnf("应用修正失败: %v, 使用原文", err)
		modifiedText = originalText
	}

	return &model.ProcessedContent{
		OriginalText: originalText,
		ModifiedText: modifiedText,
		PID:          verifyContent.TaskID,
	}, nil
}

// applyCorrections 根据 checkresultjson 将错误词替换回原文
func (p *ContentProcessor) applyCorrections(originalText string, checkResultJSON interface{}) (string, error) {
	if checkResultJSON == nil {
		return originalText, nil
	}

	// 将 checkResultJSON 转换为字符串或字节数组
	var jsonBytes []byte
	var err error

	switch v := checkResultJSON.(type) {
	case string:
		jsonBytes = []byte(v)
	case []byte:
		jsonBytes = v
	case map[string]interface{}:
		jsonBytes, err = json.Marshal(v)
		if err != nil {
			return originalText, err
		}
	default:
		jsonBytes, err = json.Marshal(v)
		if err != nil {
			return originalText, err
		}
	}

	// 解析错误信息（checkresultjson 是一个 JSON 字符串）
	var corrections []Correction
	if err := json.Unmarshal(jsonBytes, &corrections); err != nil {
		return originalText, fmt.Errorf("解析 checkresultjson 失败: %v", err)
	}

	if len(corrections) == 0 {
		return originalText, nil
	}

	// 按位置从后往前排序，避免替换时位置偏移
	sort.Slice(corrections, func(i, j int) bool {
		return corrections[i].Pos > corrections[j].Pos
	})

	// 应用修正
	result := originalText
	for _, corr := range corrections {
		// 获取正确词（corword 是数组，取第一个）
		correctWord := corr.ErrWord // 默认使用错误词（如果没有正确词）
		if len(corr.CorWord) > 0 && corr.CorWord[0] != "" {
			correctWord = corr.CorWord[0]
		}

		if corr.ErrWord == "" {
			continue
		}

		// 根据位置替换
		if corr.Pos >= 0 && corr.Pos < len(result) {
			// 检查位置是否匹配
			if corr.Pos+len(corr.ErrWord) <= len(result) {
				actualText := result[corr.Pos : corr.Pos+len(corr.ErrWord)]
				if actualText == corr.ErrWord {
					// 位置匹配，直接替换
					result = result[:corr.Pos] + correctWord + result[corr.Pos+len(corr.ErrWord):]
				} else {
					// 位置不匹配，尝试在整个文本中查找并替换第一个匹配的
					result = strings.Replace(result, corr.ErrWord, correctWord, 1)
				}
			} else {
				// 位置超出范围，尝试在整个文本中查找并替换
				result = strings.Replace(result, corr.ErrWord, correctWord, 1)
			}
		} else {
			// 位置无效，直接替换第一个匹配的
			result = strings.Replace(result, corr.ErrWord, correctWord, 1)
		}
	}

	return result, nil
}

// Correction 表示一个修正项（根据实际 JSON 结构）
type Correction struct {
	ErrType int      `json:"errtype"` // 错误类型
	ErrWord string   `json:"errword"` // 错误词
	ErrDesc string   `json:"errdesc"` // 错误描述
	Pos     int      `json:"pos"`     // 错误位置
	Level   int      `json:"level"`   // 级别
	CorWord []string `json:"corword"` // 正确词数组
}
