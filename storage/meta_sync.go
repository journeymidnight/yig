package storage

import (
	"time"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta"
)

// Remove
// 1. deleted objects
// 2. objects that already stored to Ceph but failed to update metadata
// asynchronously

const (
	META_SYNC_QUEUE_SIZE    = 100
	META_SYNC_MAX_TRY_TIMES = 3
)

func initializeMetaSyncWorker(yig *YigStorage) {
	if meta.MetaSyncQueue == nil {
		meta.MetaSyncQueue = make(chan meta.SyncEvent, META_SYNC_QUEUE_SIZE)
	}
	go metaSync(yig)
}

func metaSync(yig *YigStorage) {
	yig.WaitGroup.Add(1)
	defer yig.WaitGroup.Done()
	for {
		select {
		case event := <-meta.MetaSyncQueue:
			err := yig.MetaStorage.Sync(event)
			if err != nil {
				event.RetryTimes += 1
				if event.RetryTimes > META_SYNC_MAX_TRY_TIMES {
					helper.Logger.Println(5, "Failed to sync meta event: ",
						event, " with error", err)
					continue
				}
				meta.MetaSyncQueue <- event
				time.Sleep(1 * time.Second)
			}
		default:
			if yig.Stopping {
				helper.Logger.Print(5, ".")
				if len(meta.MetaSyncQueue) == 0 {
					return
				}
			}
			time.Sleep(5 * time.Second)
		}
	}
}
