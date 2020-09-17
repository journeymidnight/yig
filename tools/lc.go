package main

import (
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/lifecycle"
	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/log"
	. "github.com/journeymidnight/yig/meta/common"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
	"github.com/robfig/cron/v3"

	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const (
	RequestMaxKeys        = 1000
	ScanLimit             = 50
	DefaultLcLogPath      = "/var/log/yig/lc.log"
	DefaultBillingLogPath = "/var/log/yig/lc_billing.log"
	TimeLayout            = "2006-01-02 15:04:05"
)

var (
	yig             *storage.YigStorage
	taskQ           chan meta.LifeCycle
	signalQueue     chan os.Signal
	waitgroup       sync.WaitGroup
	lcHandlerIsOver bool
	stop            bool
	billingLogger   log.Logger
)

func LogBilling(userID, bucketName, objectName string,
	versionID, uploadID string, delta map[StorageClass]int64,
	// surviveTime: storage class -> remaining life in sec
	surviveTime map[StorageClass]int64) {
	timeString := time.Now().Format(TimeLayout)
	objectName = url.PathEscape(objectName) // in case there's a space in object name
	deltaStrings := make([]string, 0, len(delta))
	for class, d := range delta {
		if surviveTime != nil {
			if sec, ok := surviveTime[class]; ok {
				deltaStrings = append(deltaStrings,
					fmt.Sprintf("%s:%d:%d", class.ToString(), d, sec))
				continue
			}
		}
		deltaStrings = append(deltaStrings,
			fmt.Sprintf("%s:%d", class.ToString(), d))
	}
	deltaString := strings.Join(deltaStrings, ",")
	versionOrUploadID := "-"
	if len(versionID) != 0 {
		versionOrUploadID = versionID
	}
	if len(uploadID) != 0 {
		versionOrUploadID = uploadID
	}
	billingLogger.Println(
		fmt.Sprintf("[%s] Delta %s %s %s %s (%s)",
			timeString, userID, bucketName, objectName, versionOrUploadID, deltaString))
}

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

		result, err := yig.MetaStorage.ScanLifeCycle(ScanLimit, marker)
		if err != nil {
			helper.Logger.Error("ScanLifeCycle failed:", err)
			lcHandlerIsOver = true
			return
		}
		for _, entry := range result.Lcs {
			taskQ <- entry
			marker = entry.BucketName
		}

		if result.Truncated == false {
			lcHandlerIsOver = true
			return
		}
	}
}

//                                                                       ---->Delete object
//                                                                       |
//                  ---->NoncurrentVersion Rules----->compute action---->|
//                  |                                                    ---->Transition object
// LC---->Rules---->|                                                    ---->Delete object
//                  |                                                    |
//                  ---->CurrentVersion Rules-------->compute action---->|
//                  |                                                    ---->Transition object
//                  |
//                  ---->AbortIncompleteMultipartUpload Rules-------->compute action----->Abort object
//
func lifecycleUnit(lc meta.LifeCycle) error {
	helper.Logger.Info("Lifecycle process...")
	bucket, err := yig.MetaStorage.GetBucket(lc.BucketName, false)
	if err != nil {
		return err
	}
	bucketLC := bucket.Lifecycle

	ncvRules, cvRules, abortMultipartRules := bucketLC.FilterRules()

	var reqCtx RequestContext
	reqCtx.BucketName = bucket.Name
	reqCtx.BucketInfo = bucket

	// noncurrent version
	if bucket.Versioning != datatype.BucketVersioningDisabled && len(ncvRules) != 0 {
		helper.Logger.Info("Noncurrent version process...")
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

		var (
			objectTool       datatype.VersionedObject
			processGroupInfo meta.VersionedListObjectsInfo
			safeGroupInfo    meta.VersionedListObjectsInfo
			isLoopOver       bool
		)
		safeGroupIsNull := true
		for {
			if safeGroupIsNull {
				if isLoopOver {
					helper.Logger.Info("Process history objects over!")
					break
				}
				processGroupInfo, err = yig.ListVersionedObjectsInternal(bucket.Name, request)
				if err != nil {
					return nil
				}
			} else {
				processGroupInfo = safeGroupInfo
			}

			if processGroupInfo.IsTruncated == true {
				request.KeyMarker = processGroupInfo.NextKeyMarker
				request.VersionIdMarker = processGroupInfo.NextVersionIdMarker
				//save next list object info to avoid can not list by without marker
				safeGroupInfo, err = yig.ListVersionedObjectsInternal(bucket.Name, request)
				if err != nil {
					return nil
				}
				safeGroupIsNull = false
				isLoopOver = false
			} else {
				safeGroupIsNull = true
				isLoopOver = true
			}

			if len(processGroupInfo.Objects) < 2 {
				break
			}
			objectTool = processGroupInfo.Objects[0]
			for _, object := range processGroupInfo.Objects[1:] {
				// pass latest object
				if object.Key != objectTool.Key {
					objectTool = object
					continue
				}
				helper.Logger.Info("Object info:", object, "\n BucketName:", bucket.Name)
				reqCtx.ObjectInfo, err = yig.MetaStorage.GetObject(bucket.Name, object.Key, object.VersionId, true)
				if err != nil {
					helper.Logger.Error(bucket.Name, object.Key, object.LastModified, err)
					continue
				}
				reqCtx.ObjectName = object.Key
				reqCtx.VersionId = reqCtx.ObjectInfo.VersionId

				// Find the action that need to be executed									          TODO: add tags
				action, storageClass := bucketLC.ComputeActionForNonCurrentVersion(reqCtx.ObjectName, nil,
					reqCtx.ObjectInfo.StorageClass.ToString(), reqCtx.ObjectInfo.LastModifiedTime, ncvRules)
				helper.Logger.Info("After ComputeActionFromNonCurrentVersion:", action, storageClass)
				// Delete or transition
				if action == lifecycle.DeleteAction {
					result, err := yig.DeleteObject(reqCtx,
						common.Credential{ExternUserId: bucket.OwnerId, ExternRootId: bucket.OwnerId})
					if err != nil {
						helper.Logger.Error(reqCtx.BucketName, reqCtx.ObjectName, reqCtx.VersionId, err)
						continue
					}
					delta := make(map[StorageClass]int64)
					delta[result.DeltaSize.StorageClass] = result.DeltaSize.Delta
					survival := make(map[StorageClass]int64)
					if unexpired, sec := reqCtx.ObjectInfo.IsUnexpired(); unexpired {
						survival[result.DeltaSize.StorageClass] = sec
					}
					LogBilling(bucket.OwnerId, reqCtx.BucketName, reqCtx.ObjectName,
						"", "", delta, survival)
				}
				if action == lifecycle.TransitionAction {
					if reqCtx.ObjectInfo.DeleteMarker {
						continue
					}
					result, err := transitionObject(reqCtx, storageClass)
					if err != nil {
						helper.Logger.Error(bucket.Name, object.Key, object.LastModified, err)
						continue
					}
					LogBilling(bucket.OwnerId, bucket.Name, object.Key,
						"", "", result.DeltaInfo, nil)
				}
			}

		}
	}

	if len(cvRules) != 0 {
		helper.Logger.Info("Current version process...")
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

		var (
			objectTool       datatype.VersionedObject
			processGroupInfo meta.VersionedListObjectsInfo
			safeGroupInfo    meta.VersionedListObjectsInfo
			isLoopOver       bool
		)
		safeGroupIsNull := true
		for {
			if safeGroupIsNull {
				if isLoopOver {
					helper.Logger.Info("Process current objects over!")
					break
				}
				processGroupInfo, err = yig.ListVersionedObjectsInternal(bucket.Name, request)
				if err != nil {
					return nil
				}
			} else {
				processGroupInfo = safeGroupInfo
			}

			if processGroupInfo.IsTruncated == true {
				request.KeyMarker = processGroupInfo.NextKeyMarker
				request.VersionIdMarker = processGroupInfo.NextVersionIdMarker
				//save next list object info to avoid can not list by without marker
				safeGroupInfo, err = yig.ListVersionedObjectsInternal(bucket.Name, request)
				if err != nil {
					return nil
				}
				safeGroupIsNull = false
				isLoopOver = false
			} else {
				safeGroupIsNull = true
				isLoopOver = true
			}
			for _, object := range processGroupInfo.Objects {
				// pass old version object
				if object.Key == objectTool.Key {
					continue
				} else {
					objectTool = object
				}
				helper.Logger.Info("Object info:", object, "\n BucketName:", bucket.Name)
				reqCtx.ObjectInfo, err = yig.MetaStorage.GetObject(bucket.Name, object.Key, object.VersionId, true)
				if err != nil {
					helper.Logger.Error(bucket.Name, object.Key, object.LastModified, err)
					continue
				}
				helper.Logger.Info("DeleteMarker:", reqCtx.ObjectInfo.DeleteMarker)

				reqCtx.ObjectName = object.Key

				// DM & have other version: continue
				// DM & have not other version: expiredObjectDeleteMarkerWork == true
				var expiredObjectDeleteMarkerWork bool
				if reqCtx.ObjectInfo.DeleteMarker {
					ok, err := checkObjectOtherVersion(commonPrefix, reqCtx)
					if err != nil {
						helper.Logger.Info("checkObjectOtherVersion err:", err)
						return nil
					}
					if ok {
						continue
					}
					expiredObjectDeleteMarkerWork = true
				}
				// Find the action that need to be executed					   TODO: add tags
				action, storageClass := bucketLC.ComputeAction(reqCtx.ObjectName, nil, reqCtx.ObjectInfo.StorageClass.ToString(),
					reqCtx.ObjectInfo.LastModifiedTime, expiredObjectDeleteMarkerWork, cvRules)
				helper.Logger.Info("After computeAction:", action, storageClass)

				// process expired object delete marker;
				// If not set expiredObjectDeleteMarker,pass process
				if action == lifecycle.DeleteVersionAction {
					reqCtx.VersionId = reqCtx.ObjectInfo.VersionId
					helper.Logger.Info("$%$%$%$% deletemarker", reqCtx.BucketName, reqCtx.ObjectName, reqCtx.VersionId)
					result, err := yig.DeleteObject(reqCtx,
						common.Credential{ExternUserId: bucket.OwnerId, ExternRootId: bucket.OwnerId})
					if err != nil {
						helper.Logger.Error(reqCtx.BucketName, reqCtx.ObjectName, reqCtx.VersionId, err)
						continue
					}
					delta := make(map[StorageClass]int64)
					delta[result.DeltaSize.StorageClass] = result.DeltaSize.Delta
					survival := make(map[StorageClass]int64)
					if unexpired, sec := reqCtx.ObjectInfo.IsUnexpired(); unexpired {
						survival[result.DeltaSize.StorageClass] = sec
					}
					LogBilling(bucket.OwnerId, reqCtx.BucketName, reqCtx.ObjectName,
						reqCtx.VersionId, "", delta, survival)
				}

				// process expired object
				if action == lifecycle.DeleteAction {
					reqCtx.VersionId = ""
					result, err := yig.DeleteObject(reqCtx,
						common.Credential{ExternUserId: bucket.OwnerId, ExternRootId: bucket.OwnerId})
					if err != nil {
						helper.Logger.Error(reqCtx.BucketName, reqCtx.ObjectName, reqCtx.VersionId, err)
						continue
					}
					delta := make(map[StorageClass]int64)
					delta[result.DeltaSize.StorageClass] = result.DeltaSize.Delta
					survival := make(map[StorageClass]int64)
					if unexpired, sec := reqCtx.ObjectInfo.IsUnexpired(); unexpired {
						survival[result.DeltaSize.StorageClass] = sec
					}
					LogBilling(bucket.OwnerId, reqCtx.BucketName, reqCtx.ObjectName,
						"", "", delta, survival)
				}
				// process transition object
				if action == lifecycle.TransitionAction {
					reqCtx.VersionId = reqCtx.ObjectInfo.VersionId
					result, err := transitionObject(reqCtx, storageClass)
					if err != nil {
						helper.Logger.Error(bucket.Name, reqCtx.ObjectName, reqCtx.ObjectInfo.LastModifiedTime, err)
						continue
					}
					LogBilling(bucket.OwnerId, bucket.Name, reqCtx.ObjectName,
						reqCtx.VersionId, "", result.DeltaInfo, nil)
				}
			}
		}
	}

	if len(abortMultipartRules) != 0 {
		helper.Logger.Info("Abort incomplete multipart upload...")
		// Calculate the common prefix of all lifecycle rules
		var prefixes []string
		for _, rule := range ncvRules {
			prefixes = append(prefixes, rule.Prefix())
		}
		commonPrefix := lifecycle.Lcp(prefixes)

		var request datatype.ListUploadsRequest
		request.MaxUploads = 1000
		request.Prefix = commonPrefix

		for {
			result, err := yig.MetaStorage.Client.ListMultipartUploads(bucket.Name, request.KeyMarker,
				request.UploadIdMarker, request.Prefix, request.Delimiter, request.EncodingType, request.MaxUploads)
			if err != nil {
				return nil
			}
			for _, object := range result.Uploads {
				helper.Logger.Info("Object info:", bucket.Name, object.Key, object.StorageClass, object.Owner, object.UploadId, object.Initiator, object.Initiated)

				lastt, err := time.Parse(time.RFC3339, object.Initiated)
				if err != nil {
					return err
				}

				action := bucketLC.ComputeActionForAbortIncompleteMultipartUpload(object.Key, nil, lastt, abortMultipartRules)
				helper.Logger.Info("After ComputeActionForAbortIncompleteMultipartUpload:", action)

				reqCtx.ObjectName = object.Key

				// process abort object
				if action == lifecycle.AbortMultipartUploadAction {
					result, err := yig.AbortMultipartUpload(reqCtx,
						common.Credential{ExternUserId: bucket.OwnerId, ExternRootId: bucket.OwnerId}, object.UploadId)
					if err != nil {
						helper.Logger.Error(bucket.Name, object.Key, object.UploadId, err)
						continue
					}
					delta := make(map[StorageClass]int64)
					delta[result.StorageClass] = result.Delta
					LogBilling(bucket.OwnerId, bucket.Name, object.Key,
						"", object.UploadId, delta, nil)
				}
			}
			if result.IsTruncated == true {
				request.KeyMarker = result.NextKeyMarker
				request.UploadIdMarker = result.UploadIdMarker
			} else {
				helper.Logger.Info("Process AbortIncompleteMultipartUpload over!")
				break
			}
		}
	}

	return nil
}

func checkObjectOtherVersion(commonPrefix string, reqCtx RequestContext) (bool, error) {
	var requestForPreviousVersion datatype.ListObjectsRequest
	requestForPreviousVersion.Versioned = false
	requestForPreviousVersion.Version = 1
	requestForPreviousVersion.MaxKeys = 1
	requestForPreviousVersion.Prefix = commonPrefix
	requestForPreviousVersion.KeyMarker = reqCtx.ObjectInfo.Name
	requestForPreviousVersion.VersionIdMarker = reqCtx.ObjectInfo.VersionId

	tempInfo, err := yig.ListVersionedObjectsInternal(reqCtx.BucketName, requestForPreviousVersion)
	if err != nil {
		return false, err
	}
	helper.Logger.Info("$%$%$%$% len(tempInfo.Objects)", len(tempInfo.Objects))
	if len(tempInfo.Objects) != 0 && tempInfo.Objects[0].Key == reqCtx.ObjectInfo.Name {
		helper.Logger.Info("$%$%$%$%", tempInfo.Objects, tempInfo.Objects[0].Key, reqCtx.ObjectInfo.Name)
		return true, nil
	} else {
		return false, nil
	}
}

func transitionObject(reqCtx RequestContext, storageClass string) (result datatype.PutObjectResult, err error) {
	sourceObject := reqCtx.ObjectInfo
	sourceBucket := reqCtx.BucketInfo
	var credential common.Credential
	credential.ExternUserId = sourceBucket.OwnerId
	credential.ExternRootId = sourceBucket.OwnerId

	var sseRequest datatype.SseRequest
	sseRequest.Type = sourceObject.SseType

	// NOT support GLACIER and lower
	if StorageClassWeight[sourceObject.StorageClass] >= StorageClassWeight[ObjectStorageClassGlacier] {
		return result, ErrInvalidLcStorageClass
	}

	targetStorageClass, err := MatchStorageClassIndex(storageClass)
	if err != nil {
		return result, err
	}

	if StorageClassWeight[targetStorageClass] <= StorageClassWeight[sourceObject.StorageClass] {
		return result, ErrInvalidLcStorageClass
	}

	var isMetadataOnly bool
	if targetStorageClass != ObjectStorageClassGlacier {
		isMetadataOnly = true
	}

	pipeReader, pipeWriter := io.Pipe()
	if !isMetadataOnly {
		go func() {
			startOffset := int64(0) // Read the whole file.
			// Get the object.
			err = yig.GetObject(sourceObject, startOffset, sourceObject.Size,
				pipeWriter, sseRequest)
			if err != nil {
				helper.Logger.Error("Unable to read an object:", err)
				pipeWriter.CloseWithError(err)
				return
			}
			pipeWriter.Close()
		}()
	}

	// Note that sourceObject and targetObject are pointers
	targetObject := &meta.Object{}
	targetObject.ACL = sourceObject.ACL
	targetObject.OwnerId = sourceObject.OwnerId
	targetObject.BucketName = sourceObject.BucketName
	targetObject.Name = sourceObject.Name
	targetObject.Size = sourceObject.Size
	targetObject.Etag = sourceObject.Etag
	targetObject.Parts = sourceObject.Parts
	targetObject.Type = sourceObject.Type
	targetObject.ObjectId = sourceObject.ObjectId
	targetObject.Pool = sourceObject.Pool
	targetObject.Location = sourceObject.Location
	targetObject.CustomAttributes = sourceObject.CustomAttributes
	targetObject.ContentType = sourceObject.ContentType
	targetObject.StorageClass = targetStorageClass
	targetObject.CreateTime = sourceObject.CreateTime

	//TODO: change version when glacier transition?
	result, err = yig.CopyObject(reqCtx, targetObject, sourceObject, pipeReader, credential, sseRequest, isMetadataOnly, true)
	if err != nil {
		return result, err
	}

	return result, nil

}

func processLifecycle(process_num int) {
	time.Sleep(time.Second * 1)
	for {
		if stop {
			helper.Logger.Info("Shutting down...")
			return
		}
		waitgroup.Add(1)
		select {
		case item := <-taskQ:
			helper.Logger.Info("process", process_num, "receive task:", item)
			err := lifecycleUnit(item)
			if err != nil {
				helper.Logger.Error("Bucket", item.BucketName, "Lifecycle process error:", err)
				waitgroup.Done()
				continue
			}
			helper.Logger.Info("Bucket lifecycle done:", item.BucketName)
		default:
			if lcHandlerIsOver == true {
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
	lcHandlerIsOver = false

	taskQ = make(chan meta.LifeCycle, ScanLimit)

	numOfWorkers := helper.CONFIG.LcThread
	helper.Logger.Info("start lc thread:", numOfWorkers)

	for i := 0; i < numOfWorkers; i++ {
		go processLifecycle(i)
	}
	go getLifeCycles()
}

func main() {
	helper.SetupConfig()
	logLevel := log.ParseLevel(helper.CONFIG.LogLevel)

	helper.Logger = log.NewFileLogger(DefaultLcLogPath, logLevel)
	billingLogger = log.NewFileLogger(DefaultBillingLogPath, logLevel)

	defer helper.Logger.Close()
	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		redis.Initialize()
		defer redis.RedisConn.Close()
	}

	helper.Logger.Info("Yig lifecycle start!")
	yig = storage.New(helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, nil)

	lc := LifecycleStart

	c := cron.New()
	if helper.CONFIG.DebugMode {
		c.AddFunc(lifecycle.DebugSpec, lc)
	} else {
		c.AddFunc(helper.CONFIG.LifecycleSpec, lc)
	}
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
