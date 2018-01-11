package main

import (
	"github.com/journeymidnight/yig/helper"
	"time"
	"github.com/journeymidnight/yig/storage"
	"github.com/journeymidnight/yig/meta"
	"github.com/journeymidnight/yig/log"
	"os"
	"context"
	"os/signal"
	"syscall"
	"sync"
	"strings"
)

const (
	MAX_TRY_TIMES      = 3
	SCAN_HBASE_LIMIT   = 50
)

var (
	RootContext = context.Background()
	logger *log.Logger
	yig *storage.YigStorage
	taskQ chan meta.GarbageCollection
	waitgroup sync.WaitGroup
	stop bool
)

func deleteFromCeph()  {
	for {
		if stop {
			helper.Logger.Print(5, ".")
			return
		}
		var (
			p	*meta.Part
			err    error
		)
		garbage := <- taskQ
		waitgroup.Add(1)
		if len(garbage.Parts) == 0 {
			err = yig.DataStorage[garbage.Location].
				Remove(garbage.Pool, garbage.ObjectId)

			if err != nil {
				if strings.Contains(err.Error(), "ret=-2") {
					goto release
				}
				helper.Logger.Println(5, "failed delete", garbage.BucketName, ":", garbage.ObjectName, ":",
					garbage.Location,":",garbage.Pool,":",garbage.ObjectId, " error:", err)
			} else {
				helper.Logger.Println(5, "success delete",garbage.BucketName, ":", garbage.ObjectName, ":",
					garbage.Location,":",garbage.Pool,":",garbage.ObjectId)
			}
		} else {
			for _, p = range garbage.Parts {
				err = yig.DataStorage[garbage.Location].
					Remove(garbage.Pool, p.ObjectId)
				if err != nil {
					if strings.Contains(err.Error(), "ret=-2") {
						goto release
					}
					helper.Logger.Println(5, "failed delete part", garbage.Location, ":", garbage.Pool, ":", p.ObjectId, " error:", err)
				} else {
					helper.Logger.Println(5, "success delete part",garbage.Location, ":", garbage.Pool, ":", p.ObjectId)
				}
			}
		}
	release:
		yig.MetaStorage.RemoveGarbageCollection(garbage)
		waitgroup.Done()
	}
}

func removeDeleted () {
	for {
		if stop {
			helper.Logger.Print(5, ".")
			return
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)
		garbages, err := yig.MetaStorage.ScanGarbageCollection(SCAN_HBASE_LIMIT)
		if err != nil {
			continue
		}
		for _, garbage := range garbages {
			taskQ <- garbage
		}
	}

}


func main() {
	helper.SetupConfig()

	f, err := os.OpenFile("delete.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file in current dir")
	}
	defer f.Close()
	stop = false
	logger = log.New(f, "[yig]", log.LstdFlags, helper.CONFIG.LogLevel)
	helper.Logger = logger
	yig = storage.New(logger, int(meta.NoCache), false, helper.CONFIG.CephConfigPattern)
	taskQ = make(chan meta.GarbageCollection, SCAN_HBASE_LIMIT)
	signal.Ignore()
	signalQueue := make(chan os.Signal)

	numOfWorkers := helper.CONFIG.GcThread
	helper.Logger.Println(5, "start gc thread:",numOfWorkers)
	for i := 0; i< numOfWorkers; i++ {
		go deleteFromCeph()
	}
	go removeDeleted()
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
			stop = true
			waitgroup.Wait()
			return
		}
	}

}
