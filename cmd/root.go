package cmd

import (
	"content-verify-log/pkg/util"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "content-verify-log",
		Short: "内容验证日志处理工具",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableNoDescFlag:   true,
			DisableDescriptions: true,
			HiddenDefaultCmd:    true,
		},
	}

	// 添加迁移子命令
	rootCmd.AddCommand(NewMigrateCommand())

	rootCmd.Run = func(cmd *cobra.Command, args []string) {
		zap.S().Info("使用 'migrate' 子命令进行数据迁移")
		cmd.Help()
	}
	rootCmd.Version = util.GetVersion().Version
	return rootCmd
}
