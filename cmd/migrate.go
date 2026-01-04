package cmd

import (
	"errors"

	"content-verify-log/config"
	"content-verify-log/pkg/db"
	"content-verify-log/pkg/service"
	"content-verify-log/pkg/signals"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func NewMigrateCommand() *cobra.Command {
	var configFilePath string
	var batchSize int

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "将 MySQL 数据迁移到 DuckDB",
		Long:  "从 MySQL 的 tbl_verify_content 表读取数据，解析 JSON 内容，处理后存储到 DuckDB",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.TryLoadFromDisk(configFilePath)
			if err != nil {
				zap.S().Errorf("读取本地配置文件错误:%s", err.Error())
				return
			}
			if errs := cfg.Validate(); len(errs) > 0 {
				zap.S().Errorf("本地配置文件验证错误:%s", errors.Join(errs...))
				return
			}

			if cfg.DuckDBConfig == nil {
				zap.S().Error("DuckDB 配置未设置")
				return
			}

			ctx := signals.SetupSignalHandler()

			// 初始化 MySQL
			if err := db.InitTiDB(cfg); err != nil {
				zap.S().Errorf("MySQL 数据库连接错误:%s", err.Error())
				return
			}

			// 初始化 DuckDB
			if err := db.InitDuckDB(cfg.DuckDBConfig); err != nil {
				zap.S().Errorf("DuckDB 连接错误:%s", err.Error())
				return
			}

			// 执行迁移
			migrationService := service.NewMigrationService()
			if err := migrationService.MigrateToDuckDB(ctx, batchSize); err != nil {
				zap.S().Errorf("迁移失败:%s", err.Error())
				return
			}

			// 显示统计信息
			count, err := migrationService.GetProcessedContentCount(ctx)
			if err != nil {
				zap.S().Warnf("获取统计信息失败:%s", err.Error())
			} else {
				zap.S().Infof("DuckDB 中已处理的内容数量: %d", count)
			}
		},
	}

	cmd.Flags().StringVarP(&configFilePath, "config", "c", "./etc/config.yaml", "配置文件路径")
	cmd.Flags().IntVarP(&batchSize, "batch-size", "b", 100, "批量处理大小")
	return cmd
}
