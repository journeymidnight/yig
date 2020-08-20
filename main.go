package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/journeymidnight/yig/compression"
	"github.com/journeymidnight/yig/crypto"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/mods"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
)

var (
	v       bool
	Version = "unknown"
	Commit  = "unknown"
)

func init() {
	flag.BoolVar(&v, "v", false, "print version and commit")
}

func printVersionInfo() {
	fmt.Println("Version:", Version)
	fmt.Println("Commit:", Commit)
}

func main() {
	flag.Parse()
	if v {
		printVersionInfo()
		os.Exit(0)
	}
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
	// error log
	if helper.CONFIG.ErrorLogPath != "" {
		helper.ErrorLogger = log.NewFileLogger(helper.CONFIG.ErrorLogPath, log.InfoLevel)
		defer helper.ErrorLogger.Close()
	}

	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		redis.Initialize()
		defer redis.RedisConn.Close()
	}

	// Read all *.so from plugins directory, and fill the variable allPlugins
	allPluginMap := mods.InitialPlugins()

	kms := crypto.NewKMS(allPluginMap)

	yig := storage.New(helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, kms)
	adminServerConfig := &adminServerConfig{
		Address: helper.CONFIG.BindAdminAddress,
		Logger:  helper.Logger,
		Yig:     yig,
	}
	if helper.CONFIG.CacheCircuitCheckInterval != 0 && helper.CONFIG.MetaCacheType != 0 {
		go func() {
			yig.PingCache(time.Duration(helper.CONFIG.CacheCircuitCheckInterval) * time.Second)
		}()
	}

	// try to create compression if it is enabled.
	if helper.CONFIG.EnableCompression == true {
		compress, err := compression.InitCompression(allPluginMap)
		if err != nil {
			helper.Logger.Error("Failed to create compression unis, err:", err)
			panic("failed to create compression unis")
		}
		if compress == nil {
			helper.Logger.Error("Failed to create compression unis, unis is nil.")
			panic("failed to create compression unis, unis is nil.")
		}
		helper.Logger.Info("Succeed to create compression unis.")
	}

	iam.InitializeIamClient(allPluginMap)

	// Add pprof handler
	if helper.CONFIG.EnablePProf {
		go func() {
			err := http.ListenAndServe("0.0.0.0:8730", nil)
			helper.Logger.Error("Start pprof err:", err)
		}()
	}

	startAdminServer(adminServerConfig)

	apiServerConfig := &ServerConfig{
		Address:      helper.CONFIG.BindApiAddress,
		KeyFilePath:  helper.CONFIG.SSLKeyPath,
		CertFilePath: helper.CONFIG.SSLCertPath,
		Logger:       helper.Logger,
		ObjectLayer:  yig,
	}
	startApiServer(apiServerConfig)

	signal.Ignore()
	signalQueue := make(chan os.Signal, 10)
	signal.Notify(signalQueue, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGHUP, syscall.SIGUSR1, syscall.SIGUSR2)
	for {
		s := <-signalQueue
		switch s {
		case syscall.SIGHUP:
			// reload config file
			helper.SetupConfig()
		case syscall.SIGUSR1: // reopen log file, for log rotate
			helper.Logger.ReopenLogFile()
		case syscall.SIGUSR2: // reopen log file, for log rotate
			helper.AccessLogger.ReopenLogFile()
		default:
			// stop YIG server, order matters
			stopAdminServer()
			stopApiServer()
			yig.Stop()
			return
		}
	}
}
