package main

import (
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/lifecycle"
	"github.com/journeymidnight/yig/crypto"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/log"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/mods"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
	"go/types"
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
	empty       bool
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
			signalQueue <- syscall.SIGQUIT
			return
		}
		for _, entry := range result.Lcs {
			taskQ <- entry
			marker = entry.BucketName
		}

		if result.Truncated == false {
			empty = true
			return
		}

	}

}

// If a rule has an empty prefix, the days in it will be consider as a default days for all objects that not specified in
// other rules. For this reason, we have two conditions to check if a object has expired and should be deleted
//
//
//
//
//  if defaultConfig == false
//                 for each rule get objects by prefix
//  iterator rules ----------------------------------> loop objects-------->delete object if expired
func lifecycleUnit(lc meta.LifeCycle) error {
	bucket, err := yig.MetaStorage.GetBucket(lc.BucketName, false)
	if err != nil {
		return err
	}
	bucketLC := bucket.Lifecycle

	ncvRules, cvRules := bucketLC.FilterRulesByNonCurrentVersion()
	if len(ncvRules) != 0 {
		// Calculate the common prefix of all lifecycle rules
		/*	var prefixes []string
			for _, rule := range ncvRules {
				prefixes = append(prefixes, rule.Prefix())
			}
			commonPrefix := lifecycle.Lcp(prefixes)

			var request datatype.ListObjectsRequest
		*/

	}

	if len(cvRules) != 0 {
		// Calculate the common prefix of all lifecycle rules
		var prefixes []string
		for _, rule := range ncvRules {
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

	if object.StorageClass == meta.ObjectStorageClassGlacier || object.StorageClass == meta.ObjectStorageClassDeepArchive {
		return result, ErrInvalidCopySourceStorageClass
	}

	targetStorageClass, err := meta.MatchStorageClassIndex(storageClass)
	if err != nil {
		return result, err
	}

	if targetStorageClass < object.StorageClass {
		return result, ErrInvalidLcStorageClass
	}

	if targetStorageClass == meta.ObjectStorageClassGlacier {
		err = yig.MetaStorage.UpdateGlacierObject(targetObject, sourceObject, true)
		if err != nil {
			helper.Logger.Error("Copy Object with same source and target with GLACIER object, sql fails:", err)
			return result, ErrInternalError
		}
		result.LastModified = targetObject.LastModifiedTime
		if bucket.Versioning == "Enabled" {
			result.VersionId = targetObject.GetVersionId()
		}
		yig.MetaStorage.Cache.Remove(redis.ObjectTable, targetObject.BucketName+":"+targetObject.Name+":")
		yig.DataCache.Remove(targetObject.BucketName + ":" + targetObject.Name + ":" + targetObject.GetVersionId())
		return result, nil
	}
	err = yig.MetaStorage.ReplaceObjectMetas(targetObject)
	if err != nil {
		helper.Logger.Error("Copy Object with same source and target, sql fails:", err)
		return result, ErrInternalError
	}
	targetObject.LastModifiedTime = time.Now().UTC()
	result.LastModified = targetObject.LastModifiedTime
	if bucket.Versioning == "Enabled" {
		result.VersionId = targetObject.GetVersionId()
	}
	yig.MetaStorage.Cache.Remove(redis.ObjectTable, targetObject.BucketName+":"+targetObject.Name+":")
	yig.DataCache.Remove(targetObject.BucketName + ":" + targetObject.Name + ":" + targetObject.GetVersionId())
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
			if empty == true {
				helper.Logger.Info("All bucket lifecycle handle complete. QUIT")
				signalQueue <- syscall.SIGQUIT
				waitgroup.Done()
				return
			}
		}
		waitgroup.Done()
	}
}

func main() {
	stop = false

	helper.SetupConfig()
	logLevel := log.ParseLevel(helper.CONFIG.LogLevel)

	helper.Logger = log.NewFileLogger(DEFAULT_LC_LOG_PATH, logLevel)
	defer helper.Logger.Close()
	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		redis.Initialize()
		defer redis.Close()
	}

	// Read all *.so from plugins directory, and fill the variable allPlugins
	allPluginMap := mods.InitialPlugins()
	kms := crypto.NewKMS(allPluginMap)

	yig = storage.New(helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, kms)
	taskQ = make(chan meta.LifeCycle, SCAN_LIMIT)
	signal.Ignore()
	signalQueue = make(chan os.Signal)

	numOfWorkers := helper.CONFIG.LcThread
	helper.Logger.Info("start lc thread:", numOfWorkers)
	empty = false
	for i := 0; i < numOfWorkers; i++ {
		go processLifecycle()
	}
	go getLifeCycles()
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
