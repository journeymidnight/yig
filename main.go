/* vim: set ts=4 shiftwidth=4 smarttab noet : */

package main

import (
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/redis"
	"git.letv.cn/yig/yig/storage"
	"log"
	"os"
)

var logger *log.Logger

func main() {
	// Errors should cause panic so as to log to stderr for function calls in main()

	helper.SetupConfig()

	f, err := os.OpenFile(helper.CONFIG.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + helper.CONFIG.LogPath)
	}
	defer f.Close()

	logger = log.New(f, "[yig]", log.LstdFlags)
	helper.Logger = logger

	redis.Initialize()
	defer redis.Close()

	yig := storage.New(logger)

	adminServerConfig := &adminServerConfig{
		Address:     helper.CONFIG.BindAdminAddress,
		Logger:      logger,
		ObjectLayer: yig,
	}
	startAdminServer(adminServerConfig)

	apiServerConfig := &ServerConfig{
		Address:      helper.CONFIG.BindApiAddress,
		KeyFilePath:  helper.CONFIG.SSLKeyPath,
		CertFilePath: helper.CONFIG.SSLCertPath,
		Logger:       logger,
		ObjectLayer:  yig,
	}
	startApiServer(apiServerConfig)
}
