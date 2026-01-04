package config

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type DuckDBConfig struct {
	DBPath string `json:"dbPath" yaml:"dbPath"` // DuckDB 数据库文件路径
}

func (d *DuckDBConfig) Validate() []error {
	var errs = make([]error, 0)
	if d.DBPath == "" {
		errs = append(errs, errors.Errorf("DuckDB 数据库路径不能为空"))
		return errs
	}

	// 确保目录存在
	dir := filepath.Dir(d.DBPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		errs = append(errs, errors.Errorf("创建 DuckDB 目录失败: %v", err))
	}

	return errs
}

func NewDefaultDuckDBConfig() *DuckDBConfig {
	return &DuckDBConfig{
		DBPath: "./data/content.duckdb",
	}
}

func (d *DuckDBConfig) DSN() string {
	return d.DBPath
}
