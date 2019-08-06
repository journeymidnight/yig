package main

import (
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/mods"
	"github.com/journeymidnight/yig/iam"
	bus "github.com/journeymidnight/yig/messagebus"
	_ "github.com/journeymidnight/yig/messagebus/kafka"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
)

var logger *log.Logger

func DumpStacks() {
	buf := make([]byte, 1<<16)
	stacklen := runtime.Stack(buf, true)
	helper.Logger.Printf(5, "=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
}

func main() {
	// Errors should cause panic so as to log to stderr for function calls in main()

	rand.Seed(time.Now().UnixNano())

	helper.SetupConfig()

	//yig log
	f, err := os.OpenFile(helper.CONFIG.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + helper.CONFIG.LogPath)
	}
	defer f.Close()

	logger = log.New(f, "[yig]", log.LstdFlags, helper.CONFIG.LogLevel)
	helper.Logger = logger
	logger.Printf(20, "YIG conf: %+v \n", helper.CONFIG)
	logger.Println(5, "YIG instance ID:", helper.CONFIG.InstanceId)

	//access log
	a, err := os.OpenFile(helper.CONFIG.AccessLogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic("Failed to open access log file " + helper.CONFIG.AccessLogPath)
	}
	defer a.Close()
	accessLogger := log.New(a, "", 0, helper.CONFIG.LogLevel)
	helper.AccessLogger = accessLogger

	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		redis.Initialize()
		defer redis.Close()
	}

	yig := storage.New(logger, helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, helper.CONFIG.CephConfigPattern)
	adminServerConfig := &adminServerConfig{
		Address: helper.CONFIG.BindAdminAddress,
		Logger:  logger,
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
			helper.Logger.Printf(2, "failed to create message bus sender, err: %v", err)
			panic("failed to create message bus sender")
		}
		if nil == messageBusSender {
			helper.Logger.Printf(2, "failed to create message bus sender, sender is nil.")
			panic("failed to create message bus sender, sender is nil.")
		}
		helper.Logger.Printf(20, "succeed to create message bus sender.")
	}


	//Read all *.so from plugins directory, and fill the varaible allPlugins
	allPluginMap := mods.InitialPlugins()

	iam.InitializeIamClient(allPluginMap)

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
