package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/cannium/meepo/cron"
	"github.com/cannium/meepo/kafka"
	"github.com/cannium/meepo/log"
	"github.com/cannium/meepo/task"
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/lifecycle"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/client/tikvclient"
	"github.com/journeymidnight/yig/meta/types"
	"time"
)

func TaskObjects() []interface{} {
	return []interface{}{
		LifecycleScanner{},
		LifecycleWorker{},
	}
}

type LifecycleScanner struct {
	tikvClient *tikvclient.TiKVClient
	producer   kafka.Producer
	logger     log.Logger
	// bucket name -> bucket object
	// no need to lock since we only have 1 goroutine
	buckets map[string]types.Bucket
}

type LifecycleJob struct {
	// object to manipulate
	Object    types.Object
	Multipart types.Multipart
	// actions to apply
	Action       lifecycle.Action
	StorageClass string
}

func (s LifecycleScanner) Name() string {
	return "lifecycle_scanner"
}

type ScannerConf struct {
	ScanIntervalSecond int
	DispatchTopic      string
	PdAddresses        []string
}

func (s LifecycleScanner) Setup(handle task.ConfigHandle) (trigger cron.Trigger,
	instanceCount int, err error) {

	var conf ScannerConf
	err = handle.ReadConfig(&conf)
	if err != nil {
		return cron.Trigger{}, 0, err
	}
	trigger = cron.Trigger{
		Every: cron.Period{
			Every: conf.ScanIntervalSecond,
			Unit:  cron.Second,
		},
	}
	return trigger, 0, nil
}

func (s LifecycleScanner) Init(handle task.Handle,
	jobID int) (task.TimedTask, error) {

	var conf ScannerConf
	err := handle.ReadConfig(&conf)
	if err != nil {
		return nil, err
	}
	tikvClient := tikvclient.NewClient(conf.PdAddresses)
	producer, err := handle.NewProducer(conf.DispatchTopic)
	if err != nil {
		return nil, err
	}
	logger := handle.GetLogger()
	go func() {
		for {
			e, ok := <-producer.Errors()
			if !ok {
				return
			}
			logger.Error("Producer error:", e)
		}
	}()
	return LifecycleScanner{
		tikvClient: tikvClient,
		logger:     logger,
		producer:   producer,
	}, nil
}

func (s LifecycleScanner) getBucket(bucketName string) (types.Bucket, error) {

	b, ok := s.buckets[bucketName]
	if ok {
		return b, nil
	}
	bucket, err := s.tikvClient.GetBucket(bucketName)
	if err != nil {
		return types.Bucket{}, err
	}
	s.buckets[bucketName] = *bucket
	return *bucket, nil
}

func (s LifecycleScanner) publishObjectJob(object types.Object,
	action lifecycle.Action, storageClass string) error {

	job := LifecycleJob{
		Object:       object,
		Action:       action,
		StorageClass: storageClass,
	}
	data, err := helper.MsgPackMarshal(job)
	if err != nil {
		return err
	}
	s.producer.Publish(object.Name, data)
	return nil
}

func (s LifecycleScanner) otherVersionExists(bucket, object, version string) (bool, error) {
	listResult, err := s.tikvClient.ListVersionedObjects(
		bucket, object, version, "", "", 1)
	if err != nil {
		return false, err
	}
	if len(listResult.Objects) != 0 && listResult.Objects[0].Key == object {
		return true, nil
	} else {
		return false, nil
	}
}

func (s LifecycleScanner) scanObjects(scanRange tikvclient.Range) error {
	// maintain this variable as cache for the "current version"
	// of object being processed, if versioned
	var currentVersionObject *types.Object
	var currentVersionObjectTheOnlyDeleteMarker bool

	return s.tikvClient.TxScanCallback(scanRange.Start, scanRange.End, nil,
		func(k, v []byte) error {
			var object types.Object
			err := helper.MsgPackUnMarshal(v, &object)
			if err != nil {
				return fmt.Errorf("msgpack unmarshal: %w", err)
			}
			bucket, err := s.getBucket(object.BucketName)
			if err != nil {
				return fmt.Errorf("get bucket: %w", err)
			}
			noncurrentVersionRules, currentVersionRules, _ :=
				bucket.Lifecycle.FilterRules()
			if bucket.Versioning == datatype.BucketVersioningDisabled {
				// no possible past versions, apply "current version" rules

				action, storageClass := bucket.Lifecycle.ComputeAction(
					object.BucketName, nil,
					object.StorageClass.ToString(),
					object.LastModifiedTime, false,
					currentVersionRules)
				if action != lifecycle.NoneAction {
					return s.publishObjectJob(object, action, storageClass)
				}
			}
			if currentVersionObject == nil ||
				currentVersionObject.BucketName != object.BucketName ||
				currentVersionObject.Name != object.Name {
				// need to fetch a latest version for this object
				currentVersionObject, err = s.tikvClient.GetLatestObjectVersion(
					object.BucketName, object.Name)
				if err != nil {
					return fmt.Errorf("get latest version: %w", err)
				}
				if currentVersionObject.DeleteMarker {
					exist, err := s.otherVersionExists(currentVersionObject.BucketName,
						currentVersionObject.Name, currentVersionObject.VersionId)
					if err != nil {
						return fmt.Errorf("other version exists: %w", err)
					}
					if !exist {
						currentVersionObjectTheOnlyDeleteMarker = true
					}
				}
			}
			if object.VersionId == currentVersionObject.VersionId {
				// apply "current version" rules

				action, storageClass := bucket.Lifecycle.ComputeAction(
					object.BucketName, nil,
					object.StorageClass.ToString(),
					object.LastModifiedTime,
					currentVersionObjectTheOnlyDeleteMarker,
					currentVersionRules)
				if action != lifecycle.NoneAction {
					return s.publishObjectJob(object, action, storageClass)
				}
			}
			// apply "non-current version" rules
			action, storageClass :=
				bucket.Lifecycle.ComputeActionForNonCurrentVersion(
					object.BucketName, nil,
					object.StorageClass.ToString(),
					// "NoncurrentDays" is days after they *become* noncurrent
					currentVersionObject.LastModifiedTime,
					noncurrentVersionRules)
			if action != lifecycle.NoneAction {
				return s.publishObjectJob(object, action, storageClass)
			}
			return nil
		})
}

func (s LifecycleScanner) publishMultipartJob(multipart types.Multipart,
	action lifecycle.Action) error {

	job := LifecycleJob{
		Multipart: multipart,
		Action:    action,
	}
	data, err := helper.MsgPackMarshal(job)
	if err != nil {
		return err
	}
	s.producer.Publish(multipart.ObjectName, data)
	return nil
}

func (s LifecycleScanner) scanMultiparts(scanRange tikvclient.Range) error {
	return s.tikvClient.TxScanCallback(scanRange.Start, scanRange.End, nil,
		func(k, v []byte) error {
			var multipart types.Multipart
			err := helper.MsgPackUnMarshal(v, &multipart)
			if err != nil {
				return fmt.Errorf("msgpack unmarshal: %w", err)
			}
			bucket, err := s.getBucket(multipart.BucketName)
			if err != nil {
				return fmt.Errorf("get bucket: %w", err)
			}
			_, _, abortMultipartRules := bucket.Lifecycle.FilterRules()
			initTime := time.Unix(int64(multipart.InitialTime)/1e9,
				int64(multipart.InitialTime)%1e9)
			action := bucket.Lifecycle.ComputeActionForAbortIncompleteMultipartUpload(
				multipart.ObjectName, nil, initTime,
				abortMultipartRules)
			if action != lifecycle.NoneAction {
				err = s.publishMultipartJob(multipart, action)
				if err != nil {
					return fmt.Errorf("publish error: %w", err)
				}
			}
			return nil
		})
}

var multipartRange = tikvclient.Range{
	Start: []byte(tikvclient.TableMultipartPrefix + tikvclient.TableSeparator), // 'm', 0x1f
	End:   []byte{'m', 0x20},
}

func (s LifecycleScanner) Run(handle task.Handle, jobMeta task.JobMeta) error {
	jobRange := tikvclient.Range{
		Start: jobMeta.StartKey,
		End:   jobMeta.EndKey,
	}
	// scan objects
	buckets, err := s.tikvClient.GetBuckets()
	if err != nil {
		return fmt.Errorf("get buckets: %w", err)
	}
	for _, bucket := range buckets {
		endKey := make([]byte, 0, len(bucket.Name)+1)
		endKey = append(endKey, []byte(bucket.Name)...)
		endKey = append(endKey, byte(0x20))
		bucketRange := tikvclient.Range{
			Start: []byte(bucket.Name + tikvclient.TableSeparator),
			End:   endKey,
		}
		scanRange := tikvclient.RangeIntersection(jobRange, bucketRange)
		if scanRange.Empty {
			continue
		}
		err = s.scanObjects(scanRange)
		if err != nil {
			return err
		}
	}
	// scan multiparts
	scanRange := tikvclient.RangeIntersection(jobRange, multipartRange)
	return s.scanMultiparts(scanRange)
}

type LifecycleWorker struct {
	logger                    log.Logger
	handle                    task.Handle
	tikvClient                tikvclient.TiKVClient
	GarbageCollectionProducer kafka.Producer
}

func (w LifecycleWorker) Name() string {
	return "lifecycle_worker"
}

type WorkerConf struct {
	DispatchTopic          string
	PartitionCount         int
	PdAddresses            []string
	GarbageCollectionTopic string
}

func (w LifecycleWorker) Setup(handle task.ConfigHandle) (topic string,
	partitions int, err error) {

	var conf WorkerConf
	err = handle.ReadConfig(&conf)
	if err != nil {
		return "", 0, err
	}
	return conf.DispatchTopic, conf.PartitionCount, nil
}

func (w LifecycleWorker) Init(handle task.Handle, jobID int) (task.AsyncTask, error) {
	return nil, nil
}

func (w LifecycleWorker) handleMessage(msg *sarama.ConsumerMessage) {
	var job LifecycleJob
	err := helper.MsgPackUnMarshal(msg.Value, &job)
	if err != nil {
		w.logger.Error("Bad message:", msg.Offset, err)
		return
	}
	switch job.Action {
	case lifecycle.DeleteAction:

	case lifecycle.DeleteVersionAction:
	case lifecycle.TransitionAction:
	case lifecycle.AbortMultipartUploadAction:
	default:
		w.logger.Error("Unsupported action:", job.Action)
		return
	}
}

func (w LifecycleWorker) Run(messages <-chan *sarama.ConsumerMessage,
	errors <-chan *sarama.ConsumerError,
	commands <-chan string,
	stopping <-chan struct{}) {

	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				return
			}
			w.handleMessage(msg)
			_ = w.handle.Save(msg.Offset, nil)
		case err, ok := <-errors:
			if !ok {
				return
			}
			w.logger.Error("Consume error:", err)
		case _, ok := <-commands:
			if !ok {
				return
			}
		case <-stopping:
			return
		}
	}
}
