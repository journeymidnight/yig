package main

import (
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/log"
	bus "github.com/journeymidnight/yig/messagebus"
	_ "github.com/journeymidnight/yig/messagebus/kafka"
	"github.com/journeymidnight/yig/mods"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
)

func DumpStacks() {
	buf := make([]byte, 1<<16)
	stackLen := runtime.Stack(buf, true)
	helper.Logger.Error("Received SIGQUIT, goroutine dump:")
	helper.Logger.Error(buf[:stackLen])
	helper.Logger.Error("*** dump end")
}

func main() {
	// Errors should cause panic so as to log to stderr for initialization functions

	rand.Seed(time.Now().UnixNano())

	helper.SetupConfig()

	// yig log
	logLevel := log.ParseLevel(helper.CONFIG.LogLevel)
	helper.Logger = log.NewFileLogger(helper.CONFIG.LogPath, logLevel)
	defer helper.Logger.Close()
	helper.Logger.Info("YIG conf:", helper.CONFIG)
	helper.Logger.Info("YIG instance ID:", helper.CONFIG.InstanceId)
	// access log
	helper.AccessLogger = log.NewFileLogger(helper.CONFIG.AccessLogPath, log.InfoLevel)
	defer helper.AccessLogger.Close()

	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		redis.Initialize()
		defer redis.Close()
	}

	yig := storage.New(helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache)
	adminServerConfig := &adminServerConfig{
		Address: helper.CONFIG.BindAdminAddress,
		Logger:  helper.Logger,
		Yig:     yig,
	}
	if redis.Pool() != nil && helper.CONFIG.CacheCircuitCheckInterval != 0 {
		go yig.PingCache(time.Duration(helper.CONFIG.CacheCircuitCheckInterval) * time.Second)
	}

	// try to create message bus sender if message bus is enabled.
	// message bus sender is singleton so create it beforehand.
	if helper.CONFIG.MsgBus.Enabled {
		messageBusSender, err := bus.GetMessageSender()
		if err != nil {
			helper.Logger.Error("Failed to create message bus sender, err:", err)
			panic("failed to create message bus sender")
		}
		if nil == messageBusSender {
			helper.Logger.Error("Failed to create message bus sender, sender is nil.")
			panic("failed to create message bus sender, sender is nil.")
		}
		helper.Logger.Info("Succeed to create message bus sender.")
	}

	// Read all *.so from plugins directory, and fill the variable allPlugins
	allPluginMap := mods.InitialPlugins()

	iam.InitializeIamClient(allPluginMap)

	startAdminServer(adminServerConfig)

	apiServerConfig := &ServerConfig{
		Address:      helper.CONFIG.BindApiAddress,
		KeyFilePath:  helper.CONFIG.SSLKeyPath,
		CertFilePath: helper.CONFIG.SSLCertPath,
		Logger:       helper.Logger,
		ObjectLayer:  yig,
	}
	startApiServer(apiServerConfig)

	// ignore signal handlers set by Iris
	signal.Ignore()
	signalQueue := make(chan os.Signal)
	signal.Notify(signalQueue, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGHUP, syscall.SIGUSR1)
	for {
		s := <-signalQueue
		switch s {
		case syscall.SIGHUP:
			// reload config file
			helper.SetupConfig()
		case syscall.SIGUSR1:
			go DumpStacks()
		default:
			// stop YIG server, order matters
			stopAdminServer()
			stopApiServer()
			yig.Stop()
			return
		}
	}
}
