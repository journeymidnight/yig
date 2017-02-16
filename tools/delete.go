package main

import (
	"legitlab.letv.cn/yig/yig/helper"
	"time"
	"legitlab.letv.cn/yig/yig/storage"
	"legitlab.letv.cn/yig/yig/meta"
	"log"
	"os"
	"context"
	"github.com/cannium/gohbase/hrpc"
	"os/signal"
	"syscall"
	"sync"
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
			helper.Logger.Print(".")
			return
		}
		var (
			put *hrpc.Mutate
			processed bool
			success bool
			p	*meta.Part
			values map[string]map[string][]byte
			err    error
		)
		garbage := <- taskQ
		now := time.Now().UTC()
		waitgroup.Add(1)
		if garbage.Status == "Deleting" { //处于Deleting状态超过60秒则重置状态为Pending
			duration := int(time.Since(garbage.MTime).Seconds())
			if duration > 60 {
				garbage.Status = "Pending"
				values, err := garbage.GetValues()
				if err != nil {
					helper.Logger.Println("GetValues error:", err)
					goto release
				}
				put, err := hrpc.NewPutStr(RootContext,
					meta.GARBAGE_COLLECTION_TABLE, garbage.Rowkey, values)
				if err != nil {
					helper.Logger.Println("NewPutStr error:", err)
					goto release
				}
				_, err = yig.MetaStorage.Hbase.Put(put)
				if err != nil {
					helper.Logger.Println("Try recover status failed:",
						"garbage collection", garbage.Rowkey)
				}
			}
			goto release
		}
		garbage.Status = "Deleting"
		garbage.MTime = now
		values, err = garbage.GetValues()
		if err != nil {
			helper.Logger.Println("GetValues error:", err)
			goto release
		}
		put, err = hrpc.NewPutStr(RootContext, meta.GARBAGE_COLLECTION_TABLE,
			garbage.Rowkey, values)
		if err != nil {
			helper.Logger.Println("NewPutStr error:", err)
			goto release
		}
		processed, err = yig.MetaStorage.Hbase.CheckAndPut(put,
			meta.GARBAGE_COLLECTION_COLUMN_FAMILY, "status", []byte("Pending"))
		if !processed || err != nil {
			helper.Logger.Println("CheckAndPut error:", processed, err)
			goto release
		}
		success = true
		if len(garbage.Parts) == 0 {
			err = yig.DataStorage[garbage.Location].
				Remove(garbage.Pool, garbage.ObjectId)
			if err != nil {
				success = false
				helper.Logger.Println("failed delete", garbage.BucketName, ":", garbage.ObjectName, ":",
					garbage.Location,":",garbage.Pool,":",garbage.ObjectId)
			} else {
				helper.Logger.Println("success delete",garbage.BucketName, ":", garbage.ObjectName, ":",
					garbage.Location,":",garbage.Pool,":",garbage.ObjectId)
			}

		} else {
			for _, p = range garbage.Parts {
				err = yig.DataStorage[p.Location].
					Remove(p.Pool, p.ObjectId)
				if err != nil {
					success = false
					helper.Logger.Println("failed delete part", p.Location, ":", p.Pool, ":", p.ObjectId)
				} else {
					helper.Logger.Println("success delete part", p.Location, ":", p.Pool, ":", p.ObjectId)
				}

			}
		}
		if success {
			yig.MetaStorage.RemoveGarbageCollection(garbage)
		} else {
			garbage.TriedTimes += 1
			if garbage.TriedTimes > MAX_TRY_TIMES {
				helper.Logger.Println("Failed to remove object in Ceph:",
					garbage)
				yig.MetaStorage.RemoveGarbageCollection(garbage)
				goto release
			}
			garbage.Status = "Pending"
			values, err := garbage.GetValues()
			if err != nil {
				helper.Logger.Println("GetValues error:", err)
				goto release
			}
			put, err := hrpc.NewPutStr(RootContext,
				meta.GARBAGE_COLLECTION_TABLE, garbage.Rowkey, values)
			if err != nil {
				helper.Logger.Println("NewPutStr error:", err)
				goto release
			}
			_, err = yig.MetaStorage.Hbase.Put(put)
			if err != nil {
				helper.Logger.Println("Inconsistent data:",
					"garbage collection", garbage.Rowkey,
					"should have status `Pending`")
			}
		}
		release:
			waitgroup.Done()
	}
}

func removeDeleted () {
	for {
		if stop {
			helper.Logger.Print(".")
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
	logger = log.New(f, "[yig]", log.LstdFlags)
	helper.Logger = logger
	yig = storage.New(logger, int(meta.NoCache), false)
	taskQ = make(chan meta.GarbageCollection, SCAN_HBASE_LIMIT)
	signal.Ignore()
	signalQueue := make(chan os.Signal)

	numOfWorkers := helper.CONFIG.GcThread
	helper.Logger.Println("start gc thread:",numOfWorkers)
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
