package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type DatabaseConfig struct {
	Driver   string `mapstructure:"driver"`
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	ServerId int    `mapstructure:"server_id"`
	Name     string `mapstructure:"name"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type ElasticConfig struct {
    Address string `mapstructure:"address"`
    Username string `mapstructure:"username"`
    Password string `mapstructure:"password"`
}

type Config struct {
	Database DatabaseConfig `mapstructure:"database"`
    Elastic ElasticConfig `mapstructure:"elastic"`
    MysqlDumpPath string `mapstructure:"mysqldump_path"`
}

func LoadConfig() (*Config, error)  {
	
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			return nil, fmt.Errorf("Fatal error: config.json not found")
		}
		return nil, fmt.Errorf(fmt.Sprintf("Fatal error reading config file: %v", err))
	}

	var appConfiguration Config

	if err := viper.Unmarshal(&appConfiguration); err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("Unable to decode config into struct: %v", err))
	}
	return &appConfiguration, nil
}
