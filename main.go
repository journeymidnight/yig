/* vim: set ts=4 shiftwidth=4 smarttab noet : */

package main

import (
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/storage"
	"log"
	"os"
)

var logger *log.Logger

func main() {
	cfg, err := helper.GetGcCfg()
	if err != nil {
		panic("Failed to get config file")
		return
	}

	helper.Cfg = &cfg

	f, err := os.OpenFile(helper.Cfg.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + helper.Cfg.LogPath)
	}
	defer f.Close()

	logger = log.New(f, "[yig]", log.LstdFlags)
	helper.Logger = logger

	yig := storage.New(logger) // New() panics if errors occur

	adminServerConfig := &adminServerConfig{
		Address:     helper.Cfg.BindAdminAddress,
		Logger:      logger,
		ObjectLayer: yig,
	}
	startAdminServer(adminServerConfig)

	apiServerConfig := &ServerConfig{
		Address:      helper.Cfg.BindApiAddress,
		KeyFilePath:  helper.Cfg.SSLKeyPath,
		CertFilePath: helper.Cfg.SSLCertPath,
		Logger:       logger,
		ObjectLayer:  yig,
	}
	startApiServer(apiServerConfig)
}
