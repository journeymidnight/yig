package restore

import (
	"github.com/bsm/redislock"
	. "github.com/journeymidnight/yig-restore/error"
	"github.com/journeymidnight/yig-restore/helper"
	"github.com/journeymidnight/yig-restore/log"
	"github.com/journeymidnight/yig-restore/meta/common"
	meta "github.com/journeymidnight/yig-restore/meta/types"
	"github.com/journeymidnight/yig-restore/redis"
	"github.com/journeymidnight/yig-restore/storage"
	"github.com/robfig/cron"
	"io"
	"sync"
	"syscall"
	"time"
)

const MAXLISTNUM = 100

var (
	YIG     ServerConfig
	Crontab *cron.Cron
)

type ServerConfig struct {
	Helper      helper.Config
	Logger      log.Logger
	ObjectLayer *storage.Storage
}

func Restore(yig ServerConfig) {
	mutexs = make(map[string]*redislock.Lock)
	go autoRefreshLock()
	yig.Logger.Info("Start yig-restore success ..")
	YIG = yig
	go YIG.ContinueRestoreNotFinished()
	Crontab = cron.New()
	if !YIG.Helper.EnableRestoreObjectCron {
		YIG.Logger.Info("Start restoration and elimination with object only once")
		YIG.OperateObject()
	} else {
		YIG.Logger.Info("Start restoration and elimination with object")
		Crontab.AddFunc(YIG.Helper.RestoreObjectSpec, YIG.OperateObject)
	}
	Crontab.Start()
}

// When the program restarts or exits abnormally, the last time the object was thawed did not complete the thawing,
// restart these freezers to complete the object thawing
func (yig ServerConfig) ContinueRestoreNotFinished() {
	yig.Logger.Info("Start last unfinished thaw")

	freezers, err := yig.ObjectLayer.MetaStorage.Client.ListFreezersNeedContinue(MAXLISTNUM, common.Status(1))
	if err != nil {
		if err == ErrNoSuchKey {
			yig.Logger.Info("No restoring freezer!")
			return
		}
		yig.Logger.Error("List freezer which not finished failed, err is:", err)
		return
	}
	var w sync.WaitGroup
	for _, freezer := range freezers {
		go yig.RestoreNotFinished(freezer, &w)
	}
	w.Wait()
	yig.Logger.Info("Finish last unfinished thaw")
}

func (yig ServerConfig) RestoreNotFinished(freezer meta.Freezer, w *sync.WaitGroup) {
	w.Add(1)
	for {
		var mutex *redislock.Lock
		var targetObject *meta.Freezer
		var pipeReader *io.PipeReader
		var pipeWriter *io.PipeWriter
		var targetBucketName, targetObjectName, targetVersion string
		var sourceObject *meta.Object
		WG.Add(1)
		mutex, err := redis.Locker.Obtain(redis.GenMutexKey(&freezer), time.Duration(helper.Conf.LockTime)*time.Minute, nil)
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
		sourceObject, err = yig.ObjectLayer.GetObjectInfo(targetBucketName, targetObjectName,
			targetVersion)
		if err != nil {
			yig.Logger.Error("Unable to fetch object info:", targetBucketName, targetObjectName,
				targetVersion, err)
			goto release
		}
		pipeReader, pipeWriter = io.Pipe()
		go func() {
			startOffset := int64(0) // Read the whole file.
			// Get the object.
			err := yig.ObjectLayer.GetObject(sourceObject, startOffset, sourceObject.Size,
				pipeWriter)
			if err != nil {
				yig.Logger.Error("Unable to read an object:", err)
				pipeWriter.CloseWithError(err)
				return
			}
			pipeWriter.Close()
		}()
		targetObject = &meta.Freezer{}
		targetObject.BucketName = targetBucketName
		targetObject.Name = targetObjectName
		targetObject.Size = sourceObject.Size
		targetObject.Etag = sourceObject.Etag
		targetObject.Parts = sourceObject.Parts
		targetObject.OwnerId = sourceObject.OwnerId
		targetObject.VersionId = sourceObject.VersionId
		targetObject.Type = sourceObject.Type
		targetObject.CreateTime = sourceObject.CreateTime
		targetObject.Parts = sourceObject.Parts
		targetObject.PartsIndex = sourceObject.PartsIndex

		err = yig.ObjectLayer.RestoreObject(targetObject, pipeReader, true)
		if err != nil {
			yig.Logger.Error("CopyObject failed:", err)
			goto release
		}
		helper.Logger.Info("RestoreObject finished", targetObject)
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

func (yig ServerConfig) OperateObject() {
	yig.Logger.Info("Start operation")
	freezers, err := yig.ObjectLayer.MetaStorage.Client.ListFreezers(MAXLISTNUM)
	if err != nil && err != ErrNoSuchKey {
		yig.Logger.Error("List freezer failed, err is:", err)
	}
	for _, freezer := range freezers {
		switch freezer.Status.ToString() {
		case "READY":
			go yig.RestoreObject(freezer)
			break
		case "FINISH":
			go yig.EliminateObject(freezer)
			break
		}
	}
}

func (yig ServerConfig) RestoreObject(freezer meta.Freezer) {
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
		mutex, err := redis.Locker.Obtain(redis.GenMutexKey(&freezer), time.Duration(helper.Conf.LockTime)*time.Minute, nil)
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

		sourceObject, err = yig.ObjectLayer.GetObjectInfo(targetBucketName, targetObjectName,
			targetVersion)
		if err != nil {
			if err == ErrNoSuchKey {
				err = yig.ObjectLayer.MetaStorage.DeleteFreezerWithoutCephObject(targetBucketName, targetObjectName, targetVersion, freezer.Type, freezer.CreateTime)
				yig.Logger.Info("Delete freezer which object had been killed:", targetBucketName, targetObjectName)
				goto release
			}
			yig.Logger.Error("Unable to fetch object info:", err)
			goto release
		}
		pipeReader, pipeWriter = io.Pipe()
		go func() {
			startOffset := int64(0) // Read the whole file.
			// Get the object.
			err := yig.ObjectLayer.GetObject(sourceObject, startOffset, sourceObject.Size,
				pipeWriter)
			if err != nil {
				yig.Logger.Error("Unable to read an object:", err)
				pipeWriter.CloseWithError(err)
				return
			}
			pipeWriter.Close()
		}()
		targetObject = &meta.Freezer{}
		targetObject.BucketName = targetBucketName
		targetObject.Name = targetObjectName
		targetObject.Size = sourceObject.Size
		targetObject.Etag = sourceObject.Etag
		targetObject.OwnerId = sourceObject.OwnerId
		targetObject.VersionId = sourceObject.VersionId
		targetObject.Type = sourceObject.Type
		targetObject.CreateTime = sourceObject.CreateTime
		targetObject.Parts = sourceObject.Parts
		targetObject.PartsIndex = sourceObject.PartsIndex

		helper.Logger.Info("============================", targetObject)
		err = yig.ObjectLayer.RestoreObject(targetObject, pipeReader, true)
		if err != nil {
			_ = yig.ObjectLayer.MetaStorage.Client.UploadFreezerStatus(targetObject.BucketName, targetObject.Name, targetObject.VersionId, 1, 0)
			yig.Logger.Error("RestoreObject failed:", err)
			goto release
		}
		helper.Logger.Info("RestoreObject finished", targetObject)
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

func (yig ServerConfig) EliminateObject(freezer meta.Freezer) {
	if isNeedEliminate(freezer.LifeTime, freezer.LastModifiedTime) {
		for {
			var err error
			WG.Add(1)
			mutex, err := redis.Locker.Obtain(redis.GenMutexKey(&freezer), time.Duration(helper.Conf.LockTime)*time.Minute, nil)
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

			err = yig.ObjectLayer.EliminateObject(&freezer)
			if err != nil {
				yig.Logger.Error("Eliminate object err:", err)
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
