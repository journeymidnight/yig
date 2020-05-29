package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/journeymidnight/yig-restore/compression"
	"github.com/journeymidnight/yig-restore/helper"
	"github.com/journeymidnight/yig-restore/log"
	"github.com/journeymidnight/yig-restore/plugins/config"
	"github.com/journeymidnight/yig-restore/redis"
	"github.com/journeymidnight/yig-restore/restore"
	"github.com/journeymidnight/yig-restore/storage"
)

var (
	AllPluginMap map[string]*config.YigPlugin
)

func main() {
	rand.Seed(time.Now().UnixNano())

	// Load configuration files
	helper.ReadConfig()

	// yig log
	logLevel := log.ParseLevel(helper.Conf.LogLevel)
	helper.Logger = log.NewFileLogger(helper.Conf.LogPath, logLevel)
	defer helper.Logger.Close()
	helper.Logger.Info("Yig-Restore conf:", helper.Conf)
	helper.Logger.Info("Yig-Restore ID:", helper.Conf.InstanceId)

	// Read all *.so from plugins directory, and fill the variable allPlugins
	AllPluginMap = config.InitialPlugins()
	if helper.Conf.EnableCompression == true {
		compress, err := compression.InitCompression(AllPluginMap)
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

	// Initialize redis connection
	redis.Initialize()
	defer redis.Close()

	yig := storage.New()
	restoreServer := &restore.ServerConfig{
		Logger:      helper.Logger,
		Helper:      helper.Conf,
		ObjectLayer: yig,
	}
	go restore.Restore(*restoreServer)

	signal.Ignore()
	restore.SignalQueue = make(chan os.Signal)
	restore.ShutDown = make(chan bool)
	signal.Notify(restore.SignalQueue, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGHUP, syscall.SIGUSR1)
	for {
		s := <-restore.SignalQueue
		switch s {
		case syscall.SIGHUP:
			fmt.Print("Recieve signal SIGHUP")
			break
		case syscall.SIGUSR1:
			fmt.Print("Recieve signal SIGUSR1")
			go DumpStacks()
			break
		case syscall.SIGQUIT:
			restore.ShutDown <- true
			restore.Crontab.Stop()
			restore.WG.Wait()
			fmt.Print("Recieve signal:", s.String())
			fmt.Print("Stop yig restore...")
			return
		default:
			restore.ShutDown <- true
			restore.Crontab.Stop()
			restore.WG.Wait()
			fmt.Print("Recieve signal:", s.String())
			fmt.Print("Stop yig restore...")
			return
		}
	}
}

func DumpStacks() {
	buf := make([]byte, 1<<16)
	stacklen := runtime.Stack(buf, true)
	helper.Logger.Info("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
}
