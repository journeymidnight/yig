package main

import (
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/lifecycle"
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
	// noncurrent version
	if len(ncvRules) != 0 {
		// Calculate the common prefix of all lifecycle rules
		//var prefixes []string
		//for _, rule := range ncvRules {
		//	prefixes = append(prefixes, rule.Prefix())
		//}
		//commonPrefix := lifecycle.Lcp(prefixes)
		//
		//var request datatype.ListObjectsRequest
		//request.Versioned = true
		//request.MaxKeys = RequestMaxKeys
		//request.Prefix = commonPrefix
		//
		//for {
		//	retObjests, _, truncated, nextMarker, nextVerIdMarker, err := yig.ListObjectsInternal(bucket.Name, request)
		//	if err != nil {
		//		return nil
		//	}
		//	for _, object := range retObjests {
		//		// Find the action that need to be executed									TODO: add tags
		//		action, storageClass := bucketLC.ComputeActionFromNonCurrentVersion(object.Name, nil, object.LastModifiedTime, cvRules)
		//
		//		//Delete or transition
		//		if action == lifecycle.DeleteAction {
		//			_, err = yig.DeleteObject(object.BucketName, object.Name, object.VersionId, common.Credential{})
		//			if err != nil {
		//				helper.Logger.Error(object.BucketName, object.Name, object.VersionId, err)
		//				continue
		//			}
		//		}
		//		if action == lifecycle.TransitionAction {
		//			_, err = transitionObject(object, storageClass, common.Credential{})
		//			if err != nil {
		//				helper.Logger.Error(object.BucketName, object.Name, object.VersionId, err)
		//				continue
		//			}
		//		}
		//	}
		//
		//	if truncated == true {
		//		request.KeyMarker = nextMarker
		//		request.VersionIdMarker = nextVerIdMarker
		//	} else {
		//		break
		//	}
		//}
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
		request.MaxKeys = RequestMaxKeys
		request.Prefix = commonPrefix

		for {
			retObjests, _, truncated, nextMarker, nextVerIdMarker, err := yig.ListObjectsInternal(bucket.Name, request)
			if err != nil {
				return nil
			}
			for _, object := range retObjests {
				// Find the action that need to be executed			TODO: add tags
				action, storageClass := bucketLC.ComputeAction(object.Name, nil, object.LastModifiedTime, cvRules)

				//Delete or transition
				if action == lifecycle.DeleteAction {
					_, err = yig.DeleteObject(object.BucketName, object.Name, object.VersionId, common.Credential{})
					if err != nil {
						helper.Logger.Error(object.BucketName, object.Name, object.VersionId, err)
						continue
					}
				}
				if action == lifecycle.TransitionAction {
					_, err = transitionObject(object, storageClass, common.Credential{})
					if err != nil {
						helper.Logger.Error(object.BucketName, object.Name, object.VersionId, err)
						continue
					}
				}
			}

			if truncated == true {
				request.KeyMarker = nextMarker
				request.VersionIdMarker = nextVerIdMarker
			} else {
				break
			}
		}

	}

	return nil
}

func transitionObject(object *meta.Object, storageClass string, credential common.Credential) (result datatype.PutObjectResult, err error) {
	var sseRequest datatype.SseRequest
	sseRequest.Type = object.SseType

	// NOT support GLACIER and lower
	if object.StorageClass >= meta.ObjectStorageClassGlacier {
		return result, ErrInvalidCopySourceStorageClass
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
	yig.DataCache.Remove(object.BucketName + ":" + object.Name + ":" + object.GetVersionId())
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
		defer redis.Close()
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
