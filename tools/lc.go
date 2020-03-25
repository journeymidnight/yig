package main

import (
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/lifecycle"
	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/log"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
	"github.com/robfig/cron"

	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	RequestMaxKeys      = 1000
	SCAN_LIMIT          = 50
	DEFAULT_LC_LOG_PATH = "/var/log/yig/lc.log"
)

var (
	yig         *storage.YigStorage
	taskQ       chan meta.LifeCycle
	signalQueue chan os.Signal
	waitgroup   sync.WaitGroup
	wait        bool
	stop        bool
)

func getLifeCycles() {
	var marker string
	helper.Logger.Info("all bucket lifecycle handle start")
	waitgroup.Add(1)
	defer waitgroup.Done()
	for {
		if stop {
			helper.Logger.Info("shutting down...")
			return
		}

		result, err := yig.MetaStorage.ScanLifeCycle(SCAN_LIMIT, marker)
		if err != nil {
			helper.Logger.Error("ScanLifeCycle failed:", err)
			wait = true
			return
		}
		for _, entry := range result.Lcs {
			taskQ <- entry
			marker = entry.BucketName
		}

		if result.Truncated == false {
			wait = true
			return
		}
	}
}

//																		 ---->Delete object
//																		 |
//					---->NoncurrentVersion Rules----->compute action---->|
//					|													 ---->Transition object
// LC---->Rules---->|													 ---->Delete object
//					| 													 |
// 					---->CurrentVersion Rules-------->compute action---->|
//																		 ---->Transition object
func lifecycleUnit(lc meta.LifeCycle) error {
	helper.Logger.Info("Lifecycle process...")
	bucket, err := yig.MetaStorage.GetBucket(lc.BucketName, false)
	if err != nil {
		return err
	}
	bucketLC := bucket.Lifecycle

	ncvRules, cvRules := bucketLC.FilterRulesByNonCurrentVersion()

	var reqCtx RequestContext
	reqCtx.BucketName = bucket.Name
	reqCtx.BucketInfo = bucket

	// noncurrent version
	if bucket.Versioning != datatype.BucketVersioningDisabled && len(ncvRules) != 0 {
		// Calculate the common prefix of all lifecycle rules
		var prefixes []string
		for _, rule := range ncvRules {
			prefixes = append(prefixes, rule.Prefix())
		}
		commonPrefix := lifecycle.Lcp(prefixes)

		var request datatype.ListObjectsRequest
		request.Versioned = false
		request.Version = 1
		request.MaxKeys = RequestMaxKeys
		request.Prefix = commonPrefix

		var objectTool datatype.VersionedObject
		for {
			info, err := yig.ListVersionedObjectsInternal(bucket.Name, request)
			if err != nil {
				return nil
			}
			objectTool = info.Objects[0]
			for _, object := range info.Objects[1:] {
				lastt, err := time.Parse(time.RFC3339, object.LastModified)
				if err != nil {
					return err
				}
				// pass latest object
				if object.Key != objectTool.Key {
					objectTool = object
					continue
				} else {
					ObjToolTime, err := time.Parse(time.RFC3339, object.LastModified)
					if err != nil {
						return err
					}
					if ObjToolTime.Before(lastt) { //objectTool keep latest version
						tempObj := objectTool
						objectTool = object
						object = tempObj
					}
				}
				// Find the action that need to be executed									TODO: add tags
				action, storageClass := bucketLC.ComputeActionFromNonCurrentVersion(object.Key, nil, object.StorageClass, lastt, cvRules)

				reqCtx.ObjectInfo, err = yig.MetaStorage.GetObject(bucket.Name, object.Key, object.VersionId, true)
				if err != nil && err != ErrNoSuchKey {
					return err
				}
				reqCtx.ObjectName = object.Key
				reqCtx.VersionId = reqCtx.ObjectInfo.VersionId

				//Delete or transition
				if action == lifecycle.DeleteAction {
					_, err = yig.DeleteObject(reqCtx, common.Credential{})
					if err != nil {
						helper.Logger.Error(reqCtx.BucketName, reqCtx.ObjectName, reqCtx.VersionId, err)
						continue
					}
				}
				if action == lifecycle.TransitionAction {
					_, err = transitionObject(reqCtx.ObjectInfo, storageClass)
					if err != nil {
						helper.Logger.Error(bucket.Name, object.Key, object.LastModified, err)
						continue
					}
				}
			}

			if info.IsTruncated == true {
				request.KeyMarker = info.NextKeyMarker
				request.VersionIdMarker = info.NextVersionIdMarker
			} else {
				break
			}
		}
	}

	if len(cvRules) != 0 {
		// Calculate the common prefix of all lifecycle rules
		var prefixes []string
		for _, rule := range cvRules {
			prefixes = append(prefixes, rule.Prefix())
		}
		commonPrefix := lifecycle.Lcp(prefixes)

		var request datatype.ListObjectsRequest
		request.Versioned = false
		request.Version = 1
		request.MaxKeys = RequestMaxKeys
		request.Prefix = commonPrefix
		for {
			info, err := yig.ListObjectsInternal(bucket, request)
			if err != nil {
				return err
			}
			for _, object := range info.Objects {
				lastt, err := time.Parse(time.RFC3339, object.LastModified)
				if err != nil {
					return err
				}
				// Find the action that need to be executed					TODO: add tags
				action, storageClass := bucketLC.ComputeAction(object.Key, nil, object.StorageClass, lastt, cvRules)
				reqCtx.ObjectInfo, err = yig.MetaStorage.GetObject(bucket.Name, object.Key, "", true)
				if err != nil {
					helper.Logger.Error(bucket.Name, object.Key, object.LastModified, err)
					continue
				}
				reqCtx.ObjectName = object.Key
				reqCtx.VersionId = ""

				//process object
				if action == lifecycle.DeleteAction {
					_, err = yig.DeleteObject(reqCtx, common.Credential{})
					if err != nil {
						helper.Logger.Error(reqCtx.BucketName, reqCtx.ObjectName, reqCtx.VersionId, err)
						continue
					}
				}
				if action == lifecycle.TransitionAction {
					_, err = transitionObject(reqCtx.ObjectInfo, storageClass)
					if err != nil {
						helper.Logger.Error(bucket.Name, object.Key, object.LastModified, err)
						continue
					}
				}
			}

			if info.IsTruncated == true {
				request.KeyMarker = info.NextMarker
			} else {
				break
			}
		}

	}

	return nil
}

func transitionObject(object *meta.Object, storageClass string) (result datatype.PutObjectResult, err error) {
	var sseRequest datatype.SseRequest
	sseRequest.Type = object.SseType

	// NOT support GLACIER and lower
	if object.StorageClass >= meta.ObjectStorageClassGlacier {
		return result, ErrInvalidLcStorageClass
	}

	targetStorageClass, err := meta.MatchStorageClassIndex(storageClass)
	if err != nil {
		return result, err
	}

	if targetStorageClass <= object.StorageClass {
		return result, ErrInvalidLcStorageClass
	}

	object.StorageClass = targetStorageClass
	//TODO:If GLACIER-->DEEP_ARCHIVE or more low,may be need to add
	err = yig.MetaStorage.ReplaceObjectMetas(object)
	if err != nil {
		helper.Logger.Error("Copy Object with same source and target, sql fails:", err)
		return result, ErrInternalError
	}
	yig.MetaStorage.Cache.Remove(redis.ObjectTable, object.BucketName+":"+object.Name+":")
	yig.DataCache.Remove(object.BucketName + ":" + object.Name + ":" + object.VersionId)
	return result, nil

}

func processLifecycle() {
	time.Sleep(time.Second * 1)
	for {
		if stop {
			helper.Logger.Info("Shutting down...")
			return
		}
		waitgroup.Add(1)
		select {
		case item := <-taskQ:
			err := lifecycleUnit(item)
			if err != nil {
				helper.Logger.Error("Bucket", item.BucketName, "Lifecycle process error:", err)
				waitgroup.Done()
				continue
			}
			helper.Logger.Info("Bucket lifecycle done:", item.BucketName)
		default:
			if wait == true {
				helper.Logger.Info("All bucket lifecycle handle complete. QUIT")
				waitgroup.Done()
				return
			}
		}
		waitgroup.Done()
	}
}

func LifecycleStart() {
	stop = false
	wait = false

	taskQ = make(chan meta.LifeCycle, SCAN_LIMIT)

	numOfWorkers := helper.CONFIG.LcThread
	helper.Logger.Info("start lc thread:", numOfWorkers)

	for i := 0; i < numOfWorkers; i++ {
		go processLifecycle()
	}
	go getLifeCycles()
}

func main() {
	helper.SetupConfig()
	logLevel := log.ParseLevel(helper.CONFIG.LogLevel)

	helper.Logger = log.NewFileLogger(DEFAULT_LC_LOG_PATH, logLevel)
	defer helper.Logger.Close()
	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		redis.Initialize()
		defer redis.CloseAll()
	}

	helper.Logger.Info("Yig lifecycle start!")
	yig = storage.New(helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, nil)

	lc := LifecycleStart

	c := cron.New()
	c.AddFunc(helper.CONFIG.LifecycleSpec, lc)
	c.Start()
	defer c.Stop()

	signal.Ignore()
	signalQueue = make(chan os.Signal)
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
			helper.Logger.Info("Stopping LC")
			stop = true
			waitgroup.Wait()
			helper.Logger.Info("Done!")
			return
		}
	}

}
