package service

import (
	"context"
	"fmt"

	"content-verify-log/pkg/db"
	"content-verify-log/pkg/model"

	"github.com/google/uuid"
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

// MigrateToDuckDB 将 MySQL 中的数据迁移到 DuckDB
func (s *MigrationService) MigrateToDuckDB(ctx context.Context, batchSize int) error {
	// 创建 DuckDB 表
	if err := s.createDuckDBTable(ctx); err != nil {
		return fmt.Errorf("创建 DuckDB 表失败: %v", err)
	}

	// 获取总数
	var total int64
	if err := db.GetDBWithContext(ctx).Model(&model.VerifyContent{}).Count(&total).Error; err != nil {
		return fmt.Errorf("统计记录数失败: %v", err)
	}

	zap.S().Infof("开始迁移数据，总计 %d 条记录", total)

	offset := 0
	processed := 0
	errors := 0

	for {
		// 批量获取数据
		var contents []model.VerifyContent
		err := db.GetDBWithContext(ctx).
			Limit(batchSize).
			Offset(offset).
			Find(&contents).Error

		if err != nil {
			if err == gorm.ErrRecordNotFound || len(contents) == 0 {
				break
			}
			zap.S().Errorf("查询数据失败: %v", err)
			errors++
			offset += batchSize
			continue
		}

		if len(contents) == 0 {
			break
		}

		// 处理并插入到 DuckDB
		for _, content := range contents {
			if err := s.processAndInsert(ctx, &content); err != nil {
				zap.S().Warnf("处理记录 ID %d 失败: %v", content.ID, err)
				errors++
				continue
			}
			processed++
		}

		zap.S().Infof("已处理 %d/%d 条记录", processed, total)

		if len(contents) < batchSize {
			break
		}

		offset += batchSize
	}

	zap.S().Infof("迁移完成: 成功 %d 条, 失败 %d 条", processed, errors)
	return nil
}

// createDuckDBTable 创建 DuckDB 表
func (s *MigrationService) createDuckDBTable(ctx context.Context) error {
	duckDB := db.GetDuckDBWithContext(ctx)
	if duckDB == nil {
		return fmt.Errorf("DuckDB 连接未初始化")
	}

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS processed_content (
			id VARCHAR PRIMARY KEY,
			original_text TEXT,
			modified_text TEXT,
			pid BIGINT
		)
	`

	_, err := duckDB.ExecContext(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("创建表失败: %v", err)
	}

	zap.S().Debug("DuckDB 表创建成功")
	return nil
}

// processAndInsert 处理单条记录并插入到 DuckDB
func (s *MigrationService) processAndInsert(ctx context.Context, verifyContent *model.VerifyContent) error {
	// 处理内容
	processed, err := s.processor.ProcessContent(verifyContent)
	if err != nil {
		return fmt.Errorf("处理内容失败: %v", err)
	}

	// 生成 UUID
	processed.ID = uuid.New().String()

	// 插入到 DuckDB
	duckDB := db.GetDuckDBWithContext(ctx)
	if duckDB == nil {
		return fmt.Errorf("DuckDB 连接未初始化")
	}

	insertSQL := `
		INSERT INTO processed_content (id, original_text, modified_text, pid)
		VALUES ($1, $2, $3, $4)
	`

	_, err = duckDB.ExecContext(ctx, insertSQL,
		processed.ID,
		processed.OriginalText,
		processed.ModifiedText,
		processed.PID,
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
