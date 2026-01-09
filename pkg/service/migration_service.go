package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"content-verify-log/pkg/db"
	"content-verify-log/pkg/model"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type MigrationService struct {
	processor *ContentProcessor
}

func NewMigrationService() *MigrationService {
	return &MigrationService{
		processor: NewContentProcessor(),
	}
}

// MigrateToDuckDB 从 DuckDB 的 tbl_verify_content 表读取数据，处理后写入 processed_content 表
func (s *MigrationService) MigrateToDuckDB(ctx context.Context, batchSize int) error {
	// 创建目标 DuckDB 表
	if err := s.createDuckDBTable(ctx); err != nil {
		return fmt.Errorf("创建 DuckDB 表失败: %v", err)
	}

	duckDB := db.GetDuckDBWithContext(ctx)
	if duckDB == nil {
		return fmt.Errorf("DuckDB 连接未初始化")
	}

	startTime := time.Now()
	offset := 0
	processed := 0
	errors := 0

	for {
		// 批量查询
		query := `SELECT id, taskId, content,
			TRY_STRPTIME(created_at, '%%d/%%m/%%Y %%H:%%M:%%S.%%f') AS created_at,
			TRY_STRPTIME(updated_at, '%%d/%%m/%%Y %%H:%%M:%%S.%%f') AS updated_at,
			TRY_STRPTIME(deleted_at, '%%d/%%m/%%Y %%H:%%M:%%S.%%f') AS deleted_at
			FROM tbl_verify_content
			WHERE taskId = '430aa1b775c143e6bfcf1d5f78c115ce'
			ORDER BY id
			LIMIT ? OFFSET ?`

		rows, err := duckDB.QueryContext(ctx, query, batchSize, offset)
		if err != nil {
			return fmt.Errorf("查询数据失败: %v", err)
		}

		var contents []model.VerifyContent
		for rows.Next() {
			var content model.VerifyContent
			var taskID sql.NullString
			var contentJSON sql.NullString
			var createdAt, updatedAt, deletedAt sql.NullTime

			if err := rows.Scan(&content.ID, &taskID, &contentJSON, &createdAt, &updatedAt, &deletedAt); err != nil {
				zap.S().Warnf("扫描记录失败: %v", err)
				errors++
				continue
			}

			if taskID.Valid {
				content.TaskID = taskID.String
			}
			if createdAt.Valid {
				content.CreatedAt = createdAt.Time
			}
			if updatedAt.Valid {
				content.UpdatedAt = updatedAt.Time
			}
			if deletedAt.Valid {
				content.DeletedAt = gorm.DeletedAt{Time: deletedAt.Time, Valid: true}
			}

			// contentJSON 必须有效且可解析
			if !contentJSON.Valid {
				zap.S().Debugf("文章 ID %d: content 为 NULL，跳过", content.ID)
				continue
			}
			content.Content.Raw = contentJSON.String

			var raw map[string]interface{}
			if err := json.Unmarshal([]byte(contentJSON.String), &raw); err != nil {
				zap.S().Debugf("文章 ID %d: content 不是合法 JSON，跳过。错误: %v", content.ID, err)
				continue
			}

			dataIface, ok := raw["data"]
			if !ok {
				zap.S().Debugf("文章 ID %d: JSON 中没有 data 字段，跳过", content.ID)
				continue
			}

			data, ok := dataIface.(map[string]interface{})
			if !ok {
				zap.S().Debugf("文章 ID %d: data 字段不是 map 类型，跳过", content.ID)
				continue
			}

			// 检查两种格式
			isOldFormat := false
			isNewFormat := false

			// 旧格式：checkresultjson 不为空
			if _, hasCheckResultStr := data["checkresultstr"]; hasCheckResultStr {
				if checkResultJSONRaw, hasCheckResultJSON := data["checkresultjson"]; hasCheckResultJSON {
					switch v := checkResultJSONRaw.(type) {
					case string:
						var arr []interface{}
						if err := json.Unmarshal([]byte(v), &arr); err == nil && len(arr) > 0 {
							isOldFormat = true
						} else {
							zap.S().Debugf("文章 ID %d: checkresultjson 字符串为空或解析失败", content.ID)
						}
					case []interface{}:
						if len(v) > 0 {
							isOldFormat = true
						} else {
							zap.S().Debugf("文章 ID %d: checkresultjson 数组为空", content.ID)
						}
					default:
						zap.S().Debugf("文章 ID %d: checkresultjson 类型未知，跳过", content.ID)
					}
				} else {
					zap.S().Debugf("文章 ID %d: 缺少 checkresultjson 字段", content.ID)
				}
			}

			// 新格式：checklist 不为空
			if _, hasReplaceText := data["replace_text"]; hasReplaceText {
				if checklistRaw, hasChecklist := data["checklist"]; hasChecklist {
					switch v := checklistRaw.(type) {
					case string:
						var arr []interface{}
						if err := json.Unmarshal([]byte(v), &arr); err == nil && len(arr) > 0 {
							isNewFormat = true
						} else {
							zap.S().Debugf("文章 ID %d: checklist 字符串为空或解析失败", content.ID)
						}
					case []interface{}:
						if len(v) > 0 {
							isNewFormat = true
						} else {
							zap.S().Debugf("文章 ID %d: checklist 数组为空", content.ID)
						}
					default:
						zap.S().Debugf("文章 ID %d: checklist 类型未知，跳过", content.ID)
					}
				} else {
					zap.S().Debugf("文章 ID %d: 缺少 checklist 字段", content.ID)
				}
			}

			if !isOldFormat && !isNewFormat {
				zap.S().Debugf("文章 ID %d: 不符合任何已知格式，跳过", content.ID)
				continue
			}

			contents = append(contents, content)
		}
		rows.Close()

		if len(contents) == 0 {
			break
		}

		for _, content := range contents {
			if err := s.processAndInsert(ctx, &content); err != nil {
				zap.S().Warnf("处理记录 ID %d 失败: %v", content.ID, err)
				errors++
				continue
			}
			processed++
		}

		offset += batchSize
	}

	zap.S().Infof("处理完成: 成功 %d 条, 失败 %d 条", processed, errors)
	zap.S().Infof("耗时：%s", time.Since(startTime))
	return nil
}

// createDuckDBTable 创建 DuckDB 表
func (s *MigrationService) createDuckDBTable(ctx context.Context) error {
	duckDB := db.GetDuckDBWithContext(ctx)
	if duckDB == nil {
		return fmt.Errorf("DuckDB 连接未初始化")
	}

	// 删除旧表（如果存在），确保使用正确的表结构
	// 这样可以处理表结构变更的情况
	_, err := duckDB.ExecContext(ctx, "DROP TABLE IF EXISTS processed_content_test")
	if err != nil {
		return fmt.Errorf("删除旧表失败: %v", err)
	}

	createTableSQL := `
		CREATE TABLE processed_content_test (
			id TEXT PRIMARY KEY,
			original_text TEXT,
			modified_text TEXT,
			pid TEXT,
			error_reason TEXT
		)
	`

	_, err = duckDB.ExecContext(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("创建表失败: %v", err)
	}

	zap.S().Debug("DuckDB 表创建成功")
	return nil
}

// processAndInsert 处理单条记录并插入到 DuckDB
func (s *MigrationService) processAndInsert(ctx context.Context, verifyContent *model.VerifyContent) error {
	// 处理内容（即使处理失败也会返回结果，包含错误原因）
	processed := s.processor.ProcessContent(verifyContent)

	// 使用源表的 ID 作为主键
	processed.ID = fmt.Sprintf("%d", verifyContent.ID)

	// 插入到 DuckDB
	duckDB := db.GetDuckDBWithContext(ctx)
	if duckDB == nil {
		return fmt.Errorf("DuckDB 连接未初始化")
	}

	insertSQL := `
		INSERT INTO processed_content_test (id, original_text, modified_text, pid, error_reason)
		VALUES (?, ?, ?, ?, ?)
	`

	_, err := duckDB.ExecContext(ctx, insertSQL,
		processed.ID,
		processed.OriginalText,
		processed.ModifiedText,
		processed.PID,
		processed.ErrorReason,
	)

	if err != nil {
		return fmt.Errorf("插入数据失败: %v", err)
	}

	return nil
}

// GetProcessedContentCount 获取已处理的内容数量
func (s *MigrationService) GetProcessedContentCount(ctx context.Context) (int64, error) {
	duckDB := db.GetDuckDBWithContext(ctx)
	if duckDB == nil {
		return 0, fmt.Errorf("DuckDB 连接未初始化")
	}

	var count int64
	err := duckDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM processed_content").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("查询数量失败: %v", err)
	}

	return count, nil
}
