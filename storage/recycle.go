package storage

import (
	"github.com/journeymidnight/yig/helper"
	"time"
)

// Remove
// 1. deleted objects
// 2. objects that already stored to Ceph but failed to update metadata
// asynchronously

const (
	RECYCLE_QUEUE_SIZE = 100
	MAX_TRY_TIMES      = 3
)

type objectToRecycle struct {
	location   string
	pool       string
	objectId   string
	triedTimes int
}

var RecycleQueue chan objectToRecycle

func initializeRecycler(yig *YigStorage) {
	if RecycleQueue == nil {
		RecycleQueue = make(chan objectToRecycle, RECYCLE_QUEUE_SIZE)
	}
	// TODO: move this part of code to an isolated daemon
	go removeFailed(yig)
}

func removeFailed(yig *YigStorage) {
	yig.WaitGroup.Add(1)
	defer yig.WaitGroup.Done()
	for {
		select {
		case object := <-RecycleQueue:
			err := yig.DataStorage[object.location].Remove(object.pool, object.objectId)
			if err != nil {
				object.triedTimes += 1
				if object.triedTimes > MAX_TRY_TIMES {
					helper.Logger.Println(5, "Failed to remove object in Ceph:",
						object.location, object.pool, object.objectId,
						"with error", err)
					continue
				}
				RecycleQueue <- object
				time.Sleep(1 * time.Second)
			}
		default:
			if yig.Stopping {
				helper.Logger.Print(5, ".")
				if len(RecycleQueue) == 0 {
					return
				}
			}
			time.Sleep(5 * time.Second)
		}
	}
}

