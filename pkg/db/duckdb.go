package db

import (
	"context"
	"database/sql"
	"sync"

	"content-verify-log/config"

	_ "github.com/marcboeker/go-duckdb"
	"go.uber.org/zap"
)

var duckDB *sql.DB
var duckDBOnce sync.Once

// InitDuckDB 初始化 DuckDB 连接
func InitDuckDB(cfg *config.DuckDBConfig) error {
	var err error
	duckDBOnce.Do(func() {
		dsn := cfg.DSN()
		duckDB, err = sql.Open("duckdb", dsn)
		if err != nil {
			zap.S().Errorf("连接 DuckDB 失败: %v", err)
			return
		}

		// 测试连接
		if err = duckDB.Ping(); err != nil {
			zap.S().Errorf("DuckDB 连接测试失败: %v", err)
			return
		}

		zap.S().Debug("DuckDB 初始化完成...")
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
