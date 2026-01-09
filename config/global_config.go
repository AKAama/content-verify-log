package config

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type IConfig interface {
	Validate() []error
}

type GlobalConfig struct {
	DuckDBConfig *DuckDBConfig `json:"duckdb" yaml:"duckdb"`
}

func (g *GlobalConfig) Validate() []error {
	var errs = make([]error, 0)
	if g.DuckDBConfig != nil {
		if es := g.DuckDBConfig.Validate(); len(es) > 0 {
			errs = append(errs, es...)
		}
	}
	return errs
}

func NewDefaultGlobalConfig() *GlobalConfig {
	return &GlobalConfig{
		DuckDBConfig: NewDefaultDuckDBConfig(),
	}
}
func TryLoadFromDisk(configFilePath string) (*GlobalConfig, error) {
	_, err := os.Stat(configFilePath)
	if err != nil {
		return nil, err
	}
	dir, file := filepath.Split(configFilePath)
	fileType := filepath.Ext(file)
	viper.AddConfigPath(dir)
	viper.SetConfigName(strings.TrimSuffix(file, fileType))
	viper.SetConfigType(strings.TrimPrefix(fileType, "."))
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	if err := viper.ReadInConfig(); err != nil {
		if errors.As(err, &viper.ConfigFileNotFoundError{}) {
			return nil, err
		}
		return nil, errors.Errorf("解析配置文件错误:%s", err.Error())
	}
	cfg := NewDefaultGlobalConfig()
	if err := viper.Unmarshal(cfg, func(config *mapstructure.DecoderConfig) {
		config.TagName = strings.TrimPrefix(fileType, ".")
	}); err != nil {
		return nil, err
	}
	return cfg, nil
}
