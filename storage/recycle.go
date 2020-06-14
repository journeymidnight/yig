package storage

import (
	"time"

	"github.com/journeymidnight/yig/helper"
	meta "github.com/journeymidnight/yig/meta/types"
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
	objectType meta.ObjectType
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
			err := yig.DataStorage[object.location].Remove(object.pool, object.objectId, object.objectType)
			if err != nil {
				object.triedTimes += 1
				if object.triedTimes > MAX_TRY_TIMES {
					helper.Logger.Warn("Failed to remove object in Ceph:",
						object.location, object.pool, object.objectId,
						"with error", err)
					continue
				}
				RecycleQueue <- object
				time.Sleep(1 * time.Second)
			}
		default:
			if yig.Stopping {
				helper.Logger.Info(
					"Service shutting down, recycle queue length:",
					len(RecycleQueue))
				if len(RecycleQueue) == 0 {
					return
				}
			}
			time.Sleep(5 * time.Second)
		}
	}
}
