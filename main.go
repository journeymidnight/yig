package main

import (
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
)

var logger *log.Logger

func main() {
	// Errors should cause panic so as to log to stderr for function calls in main()

	rand.Seed(time.Now().UnixNano())

	helper.SetupConfig()

	f, err := os.OpenFile(helper.CONFIG.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + helper.CONFIG.LogPath)
	}
	defer f.Close()

	logger = log.New(f, "[yig]", log.LstdFlags, helper.CONFIG.LogLevel)
	helper.Logger = logger

	logger.Println(5, "YIG instance ID:", helper.CONFIG.InstanceId)

	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		defer redis.Close()
		redis.Initialize()
	}

	yig := storage.New(logger, helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, helper.CONFIG.CephConfigPattern)
	adminServerConfig := &adminServerConfig{
		Address: helper.CONFIG.BindAdminAddress,
		Logger:  logger,
		Yig:     yig,
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

	// ignore signal handlers set by Iris
	signal.Ignore()
	signalQueue := make(chan os.Signal)
	signal.Notify(signalQueue, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGHUP)
	for {
		s := <-signalQueue
		switch s {
		case syscall.SIGHUP:
			// reload config file
			helper.SetupConfig()
		default:
			// stop YIG server, order matters
			stopAdminServer()
			stopApiServer()
			yig.Stop()
			return
		}
	}
}
