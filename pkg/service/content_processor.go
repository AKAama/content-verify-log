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
// 支持两种格式：
// 1. 旧格式：checkresultstr + checkresultjson
// 2. 新格式：replace_text + checklist
// 即使处理失败也会返回结果，错误原因记录在 ErrorReason 字段中
func (p *ContentProcessor) ProcessContent(verifyContent *model.VerifyContent) *model.ProcessedContent {
	result := &model.ProcessedContent{
		PID: verifyContent.TaskID,
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

	// 检测格式：新格式有 replace_text 字段
	if replaceText, exists := dataObj["replace_text"].(string); exists && replaceText != "" {
		return p.processNewFormat(dataObj, result)
	}

	// 旧格式处理
	return p.processOldFormat(dataObj, result)
}

// processOldFormat 处理旧格式（checkresultstr + checkresultjson）
func (p *ContentProcessor) processOldFormat(dataObj map[string]interface{}, result *model.ProcessedContent) *model.ProcessedContent {
	// 提取 checkresultstr（原文，包含错误标记的 HTML）
	originalTextWithErrorMarkers, ok := dataObj["checkresultstr"].(string)
	if !ok {
		// 尝试其他可能的字段名
		if val, exists := dataObj["checkResultStr"]; exists {
			originalTextWithErrorMarkers, _ = val.(string)
		} else if val, exists := dataObj["check_result_str"]; exists {
			originalTextWithErrorMarkers, _ = val.(string)
		}
		if originalTextWithErrorMarkers == "" {
			result.ErrorReason = "未找到原文字段 checkresultstr"
			return result
		}
	}

	// 移除错误标记的 HTML，保留原文 HTML
	originalText := p.stripErrorMarkers(originalTextWithErrorMarkers, "old")
	// 清洗所有 HTML 标签用于存储
	result.OriginalText = p.stripHTML(originalText)

	// 提取 checkresultjson（错误信息）
	checkResultJSON, ok := dataObj["checkresultjson"]

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

	// 处理修改后的文章（在移除错误标记后的文本上应用修正）
	modifiedText, err := p.applyCorrections(originalTextWithErrorMarkers, checkResultJSON, result)
	if err != nil {
		result.ErrorReason = fmt.Sprintf("应用修正失败: %v", err)
		// ModifiedText 保持为空
		return result
	}

	// 清洗所有 HTML 标签用于存储
	result.ModifiedText = p.stripHTML(modifiedText)
	return result
}

// processNewFormat 处理新格式（replace_text + checklist）
func (p *ContentProcessor) processNewFormat(dataObj map[string]interface{}, result *model.ProcessedContent) *model.ProcessedContent {
	// 提取 replace_text（修改后的文本，包含 HTML 标记）
	replaceText, ok := dataObj["replace_text"].(string)
	if !ok || replaceText == "" {
		result.ErrorReason = "未找到 replace_text 字段或字段为空"
		return result
	}

	// 提取 checklist（错误列表）
	checklist, ok := dataObj["checklist"]
	if !ok {
		result.ErrorReason = "未找到 checklist 字段"
		// 即使没有 checklist，也可以处理 replace_text
		// 移除错误标记的html标签
		cleanedText := p.stripErrorMarkers(replaceText, "new")
		//清洗原文的html标签
		result.ModifiedText = p.stripHTML(cleanedText)
		result.OriginalText = result.ModifiedText
		return result
	}

	// 移除错误标记，保留原文 HTML
	cleanedReplaceText := p.stripErrorMarkers(replaceText, "new")
	// 对原文清洗所有 HTML 标签用于存储
	result.OriginalText = p.stripHTML(cleanedReplaceText)

	// 根据 replace_text 和 checklist组成修改后的文章
	modifiedText, err := p.applyChecklistFixes(cleanedReplaceText, checklist)
	if err != nil {
		result.ErrorReason = fmt.Sprintf("提取原文失败: %v", err)
		// 如果提取失败
		result.ModifiedText = result.OriginalText
		return result
	}

	// 移除错误标记后清洗 HTML
	cleanedModifiedText := p.stripErrorMarkers(modifiedText, "new")
	result.ModifiedText = p.stripHTML(cleanedModifiedText)

	// 检查是否有错误
	checklistArray, ok := checklist.([]interface{})
	if !ok {
		// 尝试解析为 JSON 字符串
		if str, ok := checklist.(string); ok {
			if err := json.Unmarshal([]byte(str), &checklistArray); err != nil {
				result.ErrorReason = "checklist 格式错误"
				return result
			}
		} else {
			result.ErrorReason = "checklist 格式错误"
			return result
		}
	}

	if len(checklistArray) == 0 {
		result.ErrorReason = "没有错误"
	}

	return result
}

// applyChecklistFixes 从新格式的 replace_text 和 checklist 中提取原文
func (p *ContentProcessor) applyChecklistFixes(replaceText string, checklist interface{}) (string, error) {
	// ⚠️ 不立即解码 HTML，position 基于原始文本
	originalText := replaceText

	// 解析 checklist
	var checklistItems []ChecklistItem
	var jsonBytes []byte
	var err error

	switch v := checklist.(type) {
	case string:
		jsonBytes = []byte(v)
	case []byte:
		jsonBytes = v
	case []interface{}:
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

	if err := json.Unmarshal(jsonBytes, &checklistItems); err != nil {
		return originalText, fmt.Errorf("解析 checklist 失败: %v", err)
	}

	if len(checklistItems) == 0 {
		// 没有错误，移除错误标记后返回
		return originalText, nil
	}

	// 按 position 从后往前排序，避免替换影响后续位置
	sort.Slice(checklistItems, func(i, j int) bool {
		return checklistItems[i].Position > checklistItems[j].Position
	})

	runes := []rune(originalText)

	for _, item := range checklistItems {
		if len(item.Suggest) == 0 {
			continue
		}

		start := item.Position
		end := start + item.Length

		// 边界保护
		if start < 0 || end > len(runes) {
			continue
		}

		// 校验原文内容，确保不误替换
		originalWord := string(runes[start:end])
		if originalWord != item.Word {
			continue
		}

		// 执行替换
		newRunes := []rune(item.Suggest[0])
		runes = append(runes[:start], append(newRunes, runes[end:]...)...)
	}

	return string(runes), nil
}

// applyCorrections 根据 checkresultjson 将错误词替换回原文（旧格式）
// originalTextWithMarkers: 包含错误标记 HTML 的原始文本（position 基于此）
// originalTextCleaned: 已移除错误标记的文本（用于实际替换操作）
func (p *ContentProcessor) applyCorrections(originalTextWithMarkers string, checkResultJSON interface{}, result *model.ProcessedContent) (string, error) {
	if checkResultJSON == nil {
		return originalTextWithMarkers, nil
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
			return originalTextWithMarkers, err
		}
	default:
		jsonBytes, err = json.Marshal(v)
		if err != nil {
			return originalTextWithMarkers, err
		}
	}

	// 解析错误信息（checkresultjson 是一个 JSON 字符串）
	var corrections []Correction
	if err := json.Unmarshal(jsonBytes, &corrections); err != nil {
		return originalTextWithMarkers, fmt.Errorf("checkresultjson格式与预期不符: %v", err)
	}

	if len(corrections) == 0 {
		// 没有错误，设置 ErrorReason
		if result != nil {
			result.ErrorReason = "没有错误"
		}
		return originalTextWithMarkers, nil
	}

	// 按位置从后往前排序，避免替换时位置偏移
	sort.Slice(corrections, func(i, j int) bool {
		return corrections[i].Pos > corrections[j].Pos
	})

	// 在包含错误标记的文本上应用修正
	// 因为 position 是基于包含错误标记的文本计算的
	modifiedText := originalTextWithMarkers
	runes := []rune(modifiedText)

	// 辅助函数：将字节位置转换为 rune 位置
	byteToRunePos := func(text string, bytePos int) int {
		if bytePos < 0 || bytePos >= len(text) {
			return -1
		}
		runePos := 0
		byteCount := 0
		for i, r := range text {
			if byteCount >= bytePos {
				return i
			}
			byteCount += len(string(r))
			runePos = i + 1
		}
		return runePos
	}

	// 应用修正
	for _, corr := range corrections {
		// 获取正确词（corword 是数组，取第一个）
		if len(corr.CorWord) == 0 || corr.CorWord[0] == "" {
			// 如果没有正确词，跳过
			continue
		}

		correctWord := corr.CorWord[0]

		if corr.ErrWord == "" {
			continue
		}

		errWordRunes := []rune(corr.ErrWord)
		correctWordRunes := []rune(correctWord)

		// 尝试使用位置信息（position 是基于包含错误标记的文本）
		if corr.Pos >= 0 {
			// 将字节位置转换为 rune 位置
			runePos := byteToRunePos(modifiedText, corr.Pos)

			if runePos >= 0 && runePos+len(errWordRunes) <= len(runes) {
				// 提取实际文本进行比较（可能包含错误标记 HTML）
				actualRunes := runes[runePos : runePos+len(errWordRunes)]
				actualText := string(actualRunes)

				// 移除错误标记后比较
				actualTextCleaned := p.stripErrorMarkers(actualText, "new")
				if actualTextCleaned == corr.ErrWord || actualText == corr.ErrWord {
					// 位置匹配，直接替换
					runes = append(
						runes[:runePos],
						append(correctWordRunes, runes[runePos+len(errWordRunes):]...)...,
					)
					modifiedText = string(runes)
					runes = []rune(modifiedText)
					continue
				}
			}
		}

		cleanedText := modifiedText
		idx := strings.Index(cleanedText, corr.ErrWord)
		if idx != -1 {
			// 找到匹配位置，需要在包含错误标记的文本中找到对应位置
			// 由于错误标记的存在，需要重新计算位置
			// 简化处理：在清理后的文本中替换，然后重新添加错误标记（如果有的话）
			cleanedText = strings.Replace(cleanedText, corr.ErrWord, correctWord, 1)
			// 注意：这里简化处理，实际应该保持错误标记的位置
			// 但为了简化，我们直接使用清理后的文本
			modifiedText = cleanedText
			runes = []rune(modifiedText)
		}
	}

	return modifiedText, nil
}

// stripErrorMarkers 移除错误标记的 HTML，保留原文的 HTML 和标签内的文字
// 错误标记包括：
// 1. 旧格式：
//   - <span style="background-color:yellow;"> 及其闭合标签
//   - <font color=...> 及其闭合标签（在错误标记中的）
//   - 【<无建议>,错误】等错误提示文本
//
// 2. 新格式：
//   - <span class="jdt_umold" ...> 及其闭合标签（保留标签内的文字）
func (p *ContentProcessor) stripErrorMarkers(text string, flag string) string {
	if text == "" {
		return text
	}

	if flag == "new" {
		// 1. 移除新格式的错误标记：<span class="jdt_umold" ...>...</span>
		// 只移除 span 标签本身，保留标签内的所有内容（包括文字和其他 HTML）
		newFormatErrorRegex := regexp.MustCompile(`<span[^>]*class\s*=\s*["']jdt_umold["'][^>]*>(.*?)</span>`)
		text = newFormatErrorRegex.ReplaceAllString(text, "$1")
	}
	if flag == "old" {
		// 2. 移除旧格式的错误标记的 span 标签及其内容（带 background-color:yellow 样式）
		// 使用非贪婪匹配，匹配从 <span style="background-color:yellow;"> 到 </span> 的内容
		oldFormatErrorSpanRegex := regexp.MustCompile(`<span[^>]*style\s*=\s*["'][^"']*background-color\s*:\s*yellow[^"']*["'][^>]*>.*?</span>`)
		text = oldFormatErrorSpanRegex.ReplaceAllStringFunc(text, func(match string) string {
			// 提取 span 标签内的文本内容（移除所有 HTML 标签）
			innerHTMLRegex := regexp.MustCompile(`<[^>]*>`)
			innerText := innerHTMLRegex.ReplaceAllString(match, "")
			// 移除错误提示文本
			errorTextRegex := regexp.MustCompile(`【[^】]*错误】`)
			innerText = errorTextRegex.ReplaceAllString(innerText, "")
			return innerText
		})
	}

	return text
}

// stripHTML 清洗所有 HTML 标签
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

	return cleaned
}

// Correction 表示一个修正项（旧格式）
type Correction struct {
	ErrType int      `json:"errtype"` // 错误类型
	ErrWord string   `json:"errword"` // 错误词
	ErrDesc string   `json:"errdesc"` // 错误描述
	Pos     int      `json:"pos"`     // 错误位置
	Level   int      `json:"level"`   // 级别
	CorWord []string `json:"corword"` // 正确词数组
}

// ChecklistItem 表示新格式的错误项
type ChecklistItem struct {
	Position             int                    `json:"position"`             // 错误位置
	Word                 string                 `json:"word"`                 // 错误词
	WordHtml             string                 `json:"wordHtml"`             // HTML 格式的错误词
	HtmlWords            []HtmlWord             `json:"htmlWords"`            // HTML 词列表
	Length               int                    `json:"length"`               // 长度
	Suggest              []string               `json:"suggest"`              // 建议词数组
	Explanation          string                 `json:"explanation"`          // 解释
	Type                 ChecklistErrorType     `json:"type"`                 // 错误类型
	Action               map[string]interface{} `json:"action"`               // 操作
	Context              string                 `json:"context"`              // 上下文
	Source               int                    `json:"source"`               // 来源
	UmErrorLevel         int                    `json:"um_error_level"`       // 错误级别
	LeaderLevel          string                 `json:"leader_level"`         // 领导级别
	SentenceErrorsNumber int                    `json:"sentenceErrorsNumber"` // 句子错误数
}

// HtmlWord 表示 HTML 词
type HtmlWord struct {
	Word     string `json:"word"`
	Position int    `json:"position"`
}

// ChecklistErrorType 表示错误类型
type ChecklistErrorType struct {
	ID       int    `json:"id"`
	BelongID int    `json:"belongId"`
	Name     string `json:"name"`
	Desc     string `json:"desc"`
}
