package db

import (
	"context"
	"database/sql"
	"sync"

	"content-verify-log/config"

	_ "github.com/duckdb/duckdb-go/v2"
	"go.uber.org/zap"
)

var duckDB *sql.DB
var duckDBOnce sync.Once

// InitDuckDB 初始化 duckdb 连接（使用 SQLite 替代 DuckDB，避免 CGO 依赖）
func InitDuckDB(cfg *config.DuckDBConfig) error {
	var err error
	duckDBOnce.Do(func() {
		duckDB, err = sql.Open("duckdb", cfg.DBPath)
		if err != nil {
			zap.S().Errorf("连接 duckdb 失败: %v", err)
			return
		}

		// 测试连接
		if err = duckDB.Ping(); err != nil {
			zap.S().Errorf("duckdb 连接测试失败: %v", err)
			return
		}

		zap.S().Debug("duckdb 初始化完成...")
	})
	return err
}

// GetDuckDB 获取 DuckDB 连接
func GetDuckDB() *sql.DB {
	return duckDB
}

// GetDuckDBWithContext 获取带上下文的 DuckDB 连接
func GetDuckDBWithContext(ctx context.Context) *sql.DB {
	return duckDB
}
