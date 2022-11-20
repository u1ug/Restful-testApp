package config

import (
	"github.com/ilyakaznacheev/cleanenv"
	"rest/pkg/logging"
	"sync"
)

type Config struct {
	IsDebug *bool `yaml:"isDebug"`
	Listen  struct {
		Type   string `yaml:"type" env-default:"port"`
		BindIP string `yaml:"bind_ip" env-default:"127.0.0.1"`
		Port   string `yaml:"port" env-default:"8080"`
	} `yaml:"listen"`
}

var instance *Config
var once sync.Once

func GetConfig() *Config {
	once.Do(func() {
		logger := logging.GetLogger()
		logger.Info("read application configuration")
		instance = &Config{}
		if err := cleanenv.ReadConfig("config.yml", instance); err != nil {
			desc, _ := cleanenv.GetDescription(instance, nil)
			logger.Info(desc)
			logger.Fatal(err)
		}
	})
	return instance
}
