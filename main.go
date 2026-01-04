package main

import (
	"os"
	"time"

	"content-verify-log/cmd"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func initZapLog() *zap.Logger {
	config := zap.NewProductionConfig()
	config.DisableStacktrace = true
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.DateTime + ".000")
	config.Encoding = "console"
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	logger, _ := config.Build()
	return logger
}
func main() {
	logger := initZapLog()
	zap.ReplaceGlobals(logger)
	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logger)
	command := cmd.NewRootCommand()
	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
