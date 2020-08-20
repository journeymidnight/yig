package main

import (
	"github.com/bsm/redislock"
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/meta/common"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
	"github.com/robfig/cron/v3"
	"math/rand"

	"io"
	"sync"
	"syscall"
	"time"
)

const (
	MAXLISTNUM    = 1000
	EliminateSpec = "@daily"
)

var (
	Crontab   *cron.Cron
	yig       *storage.YigStorage
	RedisConn redis.Redis
	Locker    *redislock.Client
)

func InitializeRedis() {
	switch helper.CONFIG.RedisStore {
	case "single":
		helper.Logger.Info("Redis Mode Single, ADDR is:", helper.CONFIG.RedisAddress)
		r := redis.InitializeSingle()
		RedisConn = r.(redis.Redis)
	case "cluster":
		helper.Logger.Info("Redis Mode Cluster, ADDRs is:", helper.CONFIG.RedisGroup)
		r := redis.InitializeCluster()
		RedisConn = r.(redis.Redis)
	}
	Locker = redislock.New(redis.RedisClient)
}

func Restore(instance *storage.YigStorage) {
	InitializeRedis()
	rand.Seed(time.Now().UnixNano())
	yig = instance
	mutexs = make(map[string]*redislock.Lock)

	go autoRefreshLock()
	helper.Logger.Info("Start yig-restore success ..")
	go ContinueRestoreNotFinished()
	go RestoreObjects()
	Crontab = cron.New()
	Crontab.AddFunc(EliminateSpec, EliminateObjects)
	Crontab.Start()
}

// When the program restarts or exits abnormally, the last time the object was thawed did not complete the thawing,
// restart these freezers to complete the object thawing
func ContinueRestoreNotFinished() {
	helper.Logger.Info("Start last unfinished thaw")

	freezers, err := yig.MetaStorage.Client.ListFreezersWithStatusAll(common.RestoreStatus(1))
	if err != nil {
		if err == ErrNoSuchKey {
			helper.Logger.Info("No restoring freezer!")
			return
		}
		helper.Logger.Error("List freezer which not finished failed, err is:", err)
		return
	}
	var w sync.WaitGroup
	for _, freezer := range freezers {
		go RestoreNotFinished(freezer, &w)
	}
	w.Wait()
	helper.Logger.Info("Finish last unfinished thaw")
}

func RestoreNotFinished(freezer meta.Freezer, w *sync.WaitGroup) {
	w.Add(1)
	for {
		var mutex *redislock.Lock
		var targetObject *meta.Freezer
		var pipeReader *io.PipeReader
		var pipeWriter *io.PipeWriter
		var targetBucketName, targetObjectName, targetVersion string
		var sourceObject *meta.Object
		WG.Add(1)
		mutex, err := Locker.Obtain(redis.GenMutexKeyForRestore(&freezer), time.Duration(helper.CONFIG.LockTime)*time.Minute, nil)
		if err == redislock.ErrNotObtained {
			helper.Logger.Error("Lock object failed:", freezer.BucketName, freezer.Name, freezer.VersionId)
			goto out
		} else if err != nil {
			helper.Logger.Error("Lock seems does not work, so quit", err.Error())
			goto quit
		}

		//add lock to mutexs map
		mux.Lock()
		mutexs[mutex.Key()] = mutex
		mux.Unlock()

		targetBucketName = freezer.BucketName
		targetObjectName = freezer.Name
		targetVersion = freezer.VersionId
		sourceObject, err = yig.GetObjectInfo(targetBucketName, targetObjectName, targetVersion, Credential{AllowOtherUserAccess: true})
		if err != nil {
			helper.Logger.Error("Unable to fetch object info:", targetBucketName, targetObjectName,
				targetVersion, err)
			goto release
		}
		pipeReader, pipeWriter = io.Pipe()
		go func() {
			startOffset := int64(0) // Read the whole file.
			// Get the object.
			err := yig.GetObject(sourceObject, startOffset, sourceObject.Size, pipeWriter, datatype.SseRequest{})
			if err != nil {
				helper.Logger.Error("Unable to read an object:", err)
				pipeWriter.CloseWithError(err)
				return
			}
			pipeWriter.Close()
		}()
		targetObject = &meta.Freezer{}
		targetObject.BucketName = targetBucketName
		targetObject.Name = targetObjectName
		targetObject.Size = sourceObject.Size
		targetObject.Parts = sourceObject.Parts
		targetObject.VersionId = sourceObject.VersionId
		targetObject.Type = sourceObject.Type
		targetObject.CreateTime = sourceObject.CreateTime
		targetObject.Parts = sourceObject.Parts
		targetObject.PartsIndex = sourceObject.PartsIndex
		targetObject.LifeTime = freezer.LifeTime
		if helper.CONFIG.FakeRestore {
			targetObject.Pool = sourceObject.Pool
			targetObject.Location = sourceObject.Location
			targetObject.ObjectId = sourceObject.ObjectId
		}

		err = yig.RestoreObject(targetObject, pipeReader, true)
		if err != nil {
			helper.Logger.Error("CopyObject failed:", err)
			goto release
		}
		helper.Logger.Info("RestoreObject finished", targetObject.Name, targetObject.BucketName, targetObject.VersionId, targetObject.LastModifiedTime, targetObject.LifeTime, targetObject.ObjectId, targetObject.Location, targetObject.Pool)
	release:
		mutex.Release()
		mux.Lock()
		delete(mutexs, mutex.Key())
		mux.Unlock()
		WG.Done()
		break
	out:
		WG.Done()
		break
	quit:
		SignalQueue <- syscall.SIGQUIT
	}
	w.Done()
}

func RestoreObjects() {
	helper.Logger.Info("Start restoration with object")
	c := time.Tick(time.Duration(10) * time.Second)
	for {
		select {
		case <-c:
			freezers, err := yig.MetaStorage.Client.ListFreezersWithStatus(MAXLISTNUM, common.ObjectNeedRestore)
			if err != nil && err != ErrNoSuchKey {
				helper.Logger.Error("List freezer failed, err is:", err)
				return
			}
			for _, freezer := range freezers {
				go RestoreObject(freezer)
			}
		case <-ShutDown:
			helper.Logger.Info("Shutting down, stop unfreeze table scan!")
		}
	}
}

func RestoreObject(freezer meta.Freezer) {
	for {
		var mutex *redislock.Lock
		var sourceObject *meta.Object
		var pipeReader *io.PipeReader
		var pipeWriter *io.PipeWriter
		var targetObject *meta.Freezer
		targetBucketName := freezer.BucketName
		targetObjectName := freezer.Name
		targetVersion := freezer.VersionId
		WG.Add(1)
		mutex, err := Locker.Obtain(redis.GenMutexKeyForRestore(&freezer), time.Duration(helper.CONFIG.LockTime)*time.Minute, nil)
		if err == redislock.ErrNotObtained {
			helper.Logger.Error("Lock object failed:", freezer.BucketName, freezer.Name, freezer.VersionId)
			goto out
		} else if err != nil {
			helper.Logger.Error("Lock seems does not work, so quit", err.Error())
			goto quit
		}
		//add lock to mutexs map
		mux.Lock()
		mutexs[mutex.Key()] = mutex
		mux.Unlock()
		if targetVersion == meta.NullVersion {
			targetVersion = ""
		}
		sourceObject, err = yig.GetObjectInfo(targetBucketName, targetObjectName, targetVersion, Credential{AllowOtherUserAccess: true})
		if err != nil {
			if err == ErrNoSuchKey {
				err = yig.MetaStorage.DeleteFreezerWithoutCephObject(targetBucketName, targetObjectName, targetVersion, freezer.Type, freezer.CreateTime)
				helper.Logger.Info("Delete freezer which object had been killed:", targetBucketName, targetObjectName)
				goto release
			}
			helper.Logger.Error("Unable to fetch object info:", err)
			goto release
		}
		pipeReader, pipeWriter = io.Pipe()
		go func() {
			startOffset := int64(0) // Read the whole file.
			// Get the object.
			err := yig.GetObject(sourceObject, startOffset, sourceObject.Size, pipeWriter, datatype.SseRequest{})
			if err != nil {
				helper.Logger.Error("Unable to read an object:", err)
				pipeWriter.CloseWithError(err)
				return
			}
			pipeWriter.Close()
		}()
		targetObject = &meta.Freezer{}
		targetObject.BucketName = targetBucketName
		targetObject.Name = targetObjectName
		targetObject.Size = sourceObject.Size
		targetObject.VersionId = sourceObject.VersionId
		targetObject.Type = sourceObject.Type
		targetObject.CreateTime = sourceObject.CreateTime
		targetObject.Parts = sourceObject.Parts
		targetObject.PartsIndex = sourceObject.PartsIndex
		targetObject.LifeTime = freezer.LifeTime
		if helper.CONFIG.FakeRestore {
			targetObject.Pool = sourceObject.Pool
			targetObject.Location = sourceObject.Location
			targetObject.ObjectId = sourceObject.ObjectId
		}

		err = yig.RestoreObject(targetObject, pipeReader, true)
		if err != nil {
			_ = yig.MetaStorage.Client.UpdateFreezerStatus(targetObject.BucketName, targetObject.Name, targetObject.VersionId, 1, 0)
			helper.Logger.Error("RestoreObject failed:", err)
			goto release
		}
		helper.Logger.Info("RestoreObject finished", targetObject.Name, targetObject.BucketName, targetObject.VersionId, targetObject.LastModifiedTime, targetObject.LifeTime, targetObject.ObjectId, targetObject.Location, targetObject.Pool)
	release:
		mutex.Release()
		mux.Lock()
		delete(mutexs, mutex.Key())
		mux.Unlock()
		WG.Done()
		break
	out:
		WG.Done()
		break
	quit:
		SignalQueue <- syscall.SIGQUIT
	}
}

func EliminateObjects() {
	helper.Logger.Info("Start elimination with object")
	freezers, err := yig.MetaStorage.Client.ListFreezersWithStatusAll(common.ObjectHasRestored)
	if err != nil && err != ErrNoSuchKey {
		helper.Logger.Error("List freezer failed, err is:", err)
		return
	}
	for _, freezer := range freezers {
		go EliminateObject(freezer)
	}
}

func EliminateObject(freezer meta.Freezer) {
	if isNeedEliminate(freezer.LifeTime, freezer.LastModifiedTime) {
		for {
			var err error
			WG.Add(1)
			mutex, err := Locker.Obtain(redis.GenMutexKeyForRestore(&freezer), time.Duration(helper.CONFIG.LockTime)*time.Minute, nil)
			if err == redislock.ErrNotObtained {
				helper.Logger.Error("Lock object failed:", freezer.BucketName, freezer.Name, freezer.VersionId)
				goto out
			} else if err != nil {
				helper.Logger.Error("Lock seems does not work, so quit", err.Error())
				goto quit
			}

			//add lock to mutexs map
			mux.Lock()
			mutexs[mutex.Key()] = mutex
			mux.Unlock()

			err = yig.EliminateObject(&freezer)
			if err != nil {
				helper.Logger.Error("Eliminate object err:", err)
				goto release
			}
			helper.Logger.Info("EliminateObject finished", freezer)
		release:
			mutex.Release()
			mux.Lock()
			delete(mutexs, mutex.Key())
			mux.Unlock()
			WG.Done()
			break
		out:
			WG.Done()
			break
		quit:
			SignalQueue <- syscall.SIGQUIT
		}
	}
}

func isNeedEliminate(lifeTime int, lastModifiedTime time.Time) bool {
	timeNow := time.Now().Unix()
	deadLine := lastModifiedTime.AddDate(0, 0, lifeTime).Unix()
	if deadLine <= timeNow {
		return true
	}
	return false
}
