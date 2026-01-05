package service

import (
	"encoding/json"
	"fmt"
	"html"
	"regexp"
	"sort"
	"strings"

	"content-verify-log/pkg/model"
)

type ContentProcessor struct{}

func NewContentProcessor() *ContentProcessor {
	return &ContentProcessor{}
}

// ProcessContent 处理验证内容，提取并处理 JSON 数据
// 即使处理失败也会返回结果，错误原因记录在 ErrorReason 字段中
func (p *ContentProcessor) ProcessContent(verifyContent *model.VerifyContent) *model.ProcessedContent {
	result := &model.ProcessedContent{
		PID: verifyContent.TaskID,
	}

	if verifyContent == nil {
		result.ErrorReason = "验证内容为空"
		return result
	}

	// 解析 JSON
	jsonData := verifyContent.Content.GetParsedContent()
	if jsonData == nil {
		// 尝试重新解析
		raw := verifyContent.Content.GetRawContent()
		if raw == "" {
			result.ErrorReason = "内容为空"
			return result
		}
		if err := json.Unmarshal([]byte(raw), &jsonData); err != nil {
			result.ErrorReason = fmt.Sprintf("JSON 解析失败: %v", err)
			return result
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
			result.ErrorReason = "未找到原文字段 checkresultstr"
			return result
		}
	}
	// 清洗 HTML 标签
	result.OriginalText = p.stripHTML(originalText)

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

	// 检查 checkresultjson 是否为空（nil 或空字符串）
	if checkResultJSON == nil {
		result.ErrorReason = "checkresultjson为空"
		// ModifiedText 保持为空
		return result
	}
	if str, ok := checkResultJSON.(string); ok && str == "" {
		result.ErrorReason = "checkresultjson为空"
		// ModifiedText 保持为空
		return result
	}

	// 处理修改后的文章
	modifiedText, err := p.applyCorrections(originalText, checkResultJSON, result)
	if err != nil {
		result.ErrorReason = fmt.Sprintf("应用修正失败: %v", err)
		// ModifiedText 保持为空
		return result
	}

	// 清洗 HTML 标签
	result.ModifiedText = p.stripHTML(modifiedText)
	return result
}

// applyCorrections 根据 checkresultjson 将错误词替换回原文
func (p *ContentProcessor) applyCorrections(originalText string, checkResultJSON interface{}, result *model.ProcessedContent) (string, error) {
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
		return originalText, fmt.Errorf("checkresultjson格式与预期不符: %v", err)
	}

	if len(corrections) == 0 {
		// 没有错误，设置 ErrorReason
		if result != nil {
			result.ErrorReason = "没有错误"
		}
		return originalText, nil
	}

	// 按位置从后往前排序，避免替换时位置偏移
	sort.Slice(corrections, func(i, j int) bool {
		return corrections[i].Pos > corrections[j].Pos
	})

	// 应用修正
	modifiedText := originalText
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
		if corr.Pos >= 0 && corr.Pos < len(modifiedText) {
			// 检查位置是否匹配
			if corr.Pos+len(corr.ErrWord) <= len(modifiedText) {
				actualText := modifiedText[corr.Pos : corr.Pos+len(corr.ErrWord)]
				if actualText == corr.ErrWord {
					// 位置匹配，直接替换
					modifiedText = modifiedText[:corr.Pos] + correctWord + modifiedText[corr.Pos+len(corr.ErrWord):]
				} else {
					// 位置不匹配，尝试在整个文本中查找并替换第一个匹配的
					modifiedText = strings.Replace(modifiedText, corr.ErrWord, correctWord, 1)
				}
			} else {
				// 位置超出范围，尝试在整个文本中查找并替换
				modifiedText = strings.Replace(modifiedText, corr.ErrWord, correctWord, 1)
			}
		} else {
			// 位置无效，直接替换第一个匹配的
			modifiedText = strings.Replace(modifiedText, corr.ErrWord, correctWord, 1)
		}
	}

	return modifiedText, nil
}

// stripHTML 清洗 HTML 标签
func (p *ContentProcessor) stripHTML(text string) string {
	if text == "" {
		return text
	}

	// 先解码 HTML 实体（如 &lt; 转为 <）
	decoded := html.UnescapeString(text)

	// 使用正则表达式移除所有 HTML 标签
	// 匹配 <...> 格式的标签，包括自闭合标签
	htmlTagRegex := regexp.MustCompile(`<[^>]*>`)
	cleaned := htmlTagRegex.ReplaceAllString(decoded, "")

	// 清理多余的空白字符（可选，根据需要决定是否保留）
	// cleaned = regexp.MustCompile(`\s+`).ReplaceAllString(cleaned, " ")
	// cleaned = strings.TrimSpace(cleaned)

	return cleaned
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
