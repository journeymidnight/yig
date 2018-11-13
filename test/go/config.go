package _go

import (
	"github.com/go-ini/ini"
	"os"
	"path"
)

type Config struct {
	AccessKey string `ini:"access_key"`
	SecretKey string `ini:"secret_key"`
	EndPoint  string `ini:"host_base"`
}

func GetDefaultConfigPath() string {
	var cfgPath string
	if value := os.Getenv("HOME"); value != "" {
		cfgPath = path.Join(value, ".s3cfg")
	}
	return cfgPath
}

// Load the config file if possible, but if there is an error return the default configuration file
func loadConfigFile(path string) (*Config, error) {
	cfg, err := ini.Load(path)
	if err != nil {
		return nil, err
	}

	config := &Config{
		AccessKey: cfg.Section("default").Key("access_key").Value(),
		SecretKey: cfg.Section("default").Key("secret_key").Value(),
		EndPoint:  cfg.Section("default").Key("host_base").Value(),
	}
	return config, nil
}
