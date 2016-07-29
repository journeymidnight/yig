/* vim: set ts=4 shiftwidth=4 smarttab noet : */

package main

import (
	"git.letv.cn/yig/yig/minio"
	"git.letv.cn/yig/yig/storage"
	"log"
	"os"
)

// TODO config file
const (
	LOGPATH                  = "/var/log/yig/yig.log"
	PANIC_LOG_PATH           = "/var/log/yig/panic.log"
	PIDFILE                  = "/var/run/yig/yig.pid"
	BUFFERSIZE               = 4 << 20 /* 4M */
	AIOCONCURRENT            = 4
	MAX_CHUNK_SIZE           = BUFFERSIZE * 2
	STRIPE_UNIT              = uint(512 << 10) /* 512K */
	OBJECT_SIZE              = uint(4 << 20)   /* 4M */
	STRIPE_COUNT             = uint(4)
	CONCURRENT_REQUEST_LIMIT = 100 // 0 for "no limit"
	BIND_ADDRESS             = "0.0.0.0:3000"

	SSL_KEY_PATH  = ""
	SSL_CERT_PATH = ""
	REGION        = "cn-bj-1"
)

func main() {
	f, err := os.OpenFile(LOGPATH, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + LOGPATH)
	}
	defer f.Close()

	logger := log.New(f, "[yig]", log.LstdFlags)

	yig := storage.New(logger) // New() panics if errors occur

	apiServerConfig := &minio.ServerConfig{
		Address:              BIND_ADDRESS,
		KeyFilePath:          SSL_KEY_PATH,
		CertFilePath:         SSL_CERT_PATH,
		Region:               REGION,
		Logger:               logger,
		ObjectLayer:          yig,
		MaxConcurrentRequest: CONCURRENT_REQUEST_LIMIT,
	}
	minio.StartApiServer(apiServerConfig)
}
