/* vim: set ts=4 shiftwidth=4 smarttab noet : */

package main

import (
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/storage"
	"log"
	"os"
)

// TODO config file
const (
	LOGPATH            = "/var/log/yig/yig.log"
	PANIC_LOG_PATH     = "/var/log/yig/panic.log"
	PIDFILE            = "/var/run/yig/yig.pid"
	BIND_ADDRESS       = "0.0.0.0:3000"
	ADMIN_BIND_ADDRESS = "0.0.0.0:9000"

	SSL_KEY_PATH  = ""
	SSL_CERT_PATH = ""
)

var logger *log.Logger

func main() {
	f, err := os.OpenFile(LOGPATH, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + LOGPATH)
	}
	defer f.Close()

	logger = log.New(f, "[yig]", log.LstdFlags)
	helper.Logger = logger

	yig := storage.New(logger) // New() panics if errors occur

	adminServerConfig := &adminServerConfig{
		Address:     ADMIN_BIND_ADDRESS,
		Logger:      logger,
		ObjectLayer: yig,
	}
	startAdminServer(adminServerConfig)

	apiServerConfig := &ServerConfig{
		Address:      BIND_ADDRESS,
		KeyFilePath:  SSL_KEY_PATH,
		CertFilePath: SSL_CERT_PATH,
		Logger:       logger,
		ObjectLayer:  yig,
	}
	startApiServer(apiServerConfig)
}
