package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/storage"
	"github.com/journeymidnight/yig/tools/restore/restore"
)

const DefaultRestoreLog = "/var/log/yig/restore.log"

func main() {
	rand.Seed(time.Now().UnixNano())

	// Load configuration files
	helper.SetupConfig()

	// yig log
	logLevel := log.ParseLevel(helper.CONFIG.LogLevel)
	helper.Logger = log.NewFileLogger(DefaultRestoreLog, logLevel)
	defer helper.Logger.Close()

	yig := storage.New(helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, nil)

	go restore.Restore(yig)

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
