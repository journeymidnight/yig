package main

import (
	"10.0.45.221/meepo/cron.git"
	"10.0.45.221/meepo/kafka.git"
	"10.0.45.221/meepo/log.git"
	"10.0.45.221/meepo/task.git"
	"github.com/Shopify/sarama"
	"github.com/bsm/redislock"
	redis2 "github.com/go-redis/redis/v7"
	"github.com/journeymidnight/yig/backend"
	"github.com/journeymidnight/yig/ceph"
	error2 "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/client/tikvclient"
	"github.com/journeymidnight/yig/meta/common"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"sync"
	"time"
)

/*
When a user append-upload a file, it would first write to an SSD pool,
after the file becoming cold (not append for some amount of time),
"migrate" the file to HDD pool.
*/

func TaskObjects() []interface{} {
	return []interface{}{
		MigrateScanner{},
		MigrateWorker{},
	}
}

type MigrateScanner struct {
	coolDown   time.Duration
	tikvClient *tikvclient.TiKVClient
	producer   kafka.Producer
	logger     log.Logger
}

func (s MigrateScanner) Name() string {
	return "migrate_scanner"
}

type ScannerConf struct {
	ScanIntervalSecond int
	CoolDownSecond     int
	DispatchTopic      string
}

func (s MigrateScanner) Setup(handle task.ConfigHandle) (trigger cron.Trigger,
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

func (s MigrateScanner) Init(handle task.Handle,
	jobID int) (task.TimedTask, error) {

	var conf ScannerConf
	err := handle.ReadConfig(&conf)
	if err != nil {
		return nil, err
	}
	tikvClient, err := handle.NewTikvClient()
	if err != nil {
		return nil, err
	}
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
	return MigrateScanner{
		coolDown:   time.Duration(conf.CoolDownSecond) * time.Second,
		tikvClient: &tikvclient.TiKVClient{TxnCli: tikvClient},
		producer:   producer,
		logger:     logger,
	}, nil
}

var hotObjectRange = tikvclient.Range{
	Start: []byte(tikvclient.TableHotObjectPrefix + tikvclient.TableSeparator), // 'h', 0x1f
	End:   []byte{'h', 0x20},
}

func (s MigrateScanner) processEntry(key []byte, value []byte) {
	var object types.Object
	err := helper.MsgPackUnMarshal(value, &object)
	if err != nil {
		s.logger.Error("Unmarshal", string(key), "error:", err)
		return
	}
	s.logger.Info("Processing:", object)
	if object.LastModifiedTime.Add(s.coolDown).After(time.Now()) {
		// not cool yet
		return
	}

	s.producer.Publish(string(key), value)
	err = s.tikvClient.RemoveHotObject(&object, nil)
	if err != nil {
		s.logger.Warn("GetCredFromIam:",
			object.BucketName, object.Name, object.VersionId,
			"err:", err)
	}
}

func (s MigrateScanner) Run(handle task.Handle, jobMeta task.JobMeta) error {
	instanceRange := tikvclient.RangeIntersection(hotObjectRange,
		tikvclient.Range{Start: jobMeta.StartKey, End: jobMeta.EndKey})
	if instanceRange.Empty {
		s.logger.Info("Intersection with", jobMeta.StartKey, jobMeta.EndKey,
			"result:", instanceRange.Start, instanceRange.End)
		return nil
	}
	return s.tikvClient.TxScanCallback(instanceRange.Start, instanceRange.End, nil,
		func(k, v []byte) error {
			s.processEntry(k, v)
			return nil
		})
}

type MigrateWorker struct {
	coolDown         time.Duration
	threadsPerWorker int
	tikvClient       *tikvclient.TiKVClient
	cephClusters     map[string]backend.Cluster
	redisClient      redis2.UniversalClient
	redisLocker      *redislock.Client
	logger           log.Logger
	handle           task.Handle
}

func (w MigrateWorker) Name() string {
	return "migrate_worker"
}

type WorkerConf struct {
	CoolDownSecond    int
	DispatchTopic     string
	ThreadsPerWorker  int
	Partitions        int
	CephConfigPattern string
}

func (w MigrateWorker) Setup(handle task.ConfigHandle) (topic string,
	partitions int, err error) {

	var conf WorkerConf
	err = handle.ReadConfig(&conf)
	if err != nil {
		return "", 0, err
	}
	return conf.DispatchTopic, conf.Partitions, nil
}

func (w MigrateWorker) Init(handle task.Handle, jobID int) (task.AsyncTask, error) {
	var conf WorkerConf
	err := handle.ReadConfig(&conf)
	if err != nil {
		return nil, err
	}
	tikvClient, err := handle.NewTikvClient()
	if err != nil {
		return nil, err
	}
	cephClusters, err := ceph.PureInitialize(conf.CephConfigPattern)
	if err != nil {
		return nil, err
	}
	redisClient := handle.NewRedisClient()
	redisLocker := redislock.New(redisClient)
	return MigrateWorker{
		coolDown:         time.Duration(conf.CoolDownSecond) * time.Second,
		threadsPerWorker: conf.ThreadsPerWorker,
		tikvClient:       &tikvclient.TiKVClient{TxnCli: tikvClient},
		cephClusters:     cephClusters,
		redisClient:      redisClient,
		redisLocker:      redisLocker,
		logger:           handle.GetLogger(),
		handle:           handle,
	}, nil
}

func (w MigrateWorker) lockEntry(object types.Object,
	lockNoLongerRequired chan struct{}, locked chan struct{}) {

	var lock *redislock.Lock
	var once sync.Once
	var err error
	for {
		select {
		case <-lockNoLongerRequired:
			return
		default:
			if lock == nil {
				lock, err = w.redisLocker.Obtain(redis.GenMutexKey(&object),
					10*time.Second, nil)
			} else {
				err = lock.Refresh(10*time.Second, nil)
			}
			if err == redislock.ErrNotObtained {
				lock = nil
				time.Sleep(10 * time.Second)
				continue
			}
			if err != nil {
				w.logger.Error("Obtain or refresh lock",
					redis.GenMutexKey(&object), "error:", err)
				time.Sleep(5 * time.Second)
				continue
			}
			once.Do(func() {
				close(locked) // signal to `processEntry` we have acquired the lock
			})
			time.Sleep(5 * time.Second)
		}
	}
}

func (w MigrateWorker) processEntry(object types.Object) {
	lockNoLongerRequired := make(chan struct{})
	defer func() {
		// signal to `lockEntry` "lock no longer required"
		close(lockNoLongerRequired)
	}()
	locked := make(chan struct{})
	w.lockEntry(object, lockNoLongerRequired, locked)
	<-locked // wait until entry lock acquired

	for {
		sourceObject, err := w.tikvClient.GetObject(
			object.BucketName, object.Name, object.VersionId, nil)
		if err != nil {
			if err == error2.ErrNoSuchKey {
				return
			}
			w.logger.Error("GetObject", object.BucketName, object.Name,
				object.VersionId, "error:", err)
			time.Sleep(time.Second)
			continue
		}
		if sourceObject.Pool == backend.BIG_FILE_POOLNAME {
			w.logger.Info("Object already migrated, so ignore it:",
				sourceObject.Name, sourceObject.Pool, sourceObject.ObjectId)
			return
		}
		if sourceObject.LastModifiedTime.Add(w.coolDown).After(time.Now()) {
			return
		}
		if sourceObject.StorageClass == common.ObjectStorageClassGlacier {
			w.logger.Info("Abort migrate because StorageClass changed for :",
				sourceObject.BucketName+":"+sourceObject.Name+":"+
					sourceObject.VersionId+":"+sourceObject.ObjectId)
			return
		}
		w.logger.Info("Start migrate for:",
			sourceObject.BucketName+":"+sourceObject.Name+":"+
				sourceObject.VersionId+":"+sourceObject.ObjectId)

		sourceCluster, ok := w.cephClusters[sourceObject.Location]
		if !ok {
			w.logger.Error("Ceph cluster", sourceObject.Location,
				"not configured")
			return
		}
		reader, err := sourceCluster.GetReader(sourceObject.Pool, sourceObject.ObjectId,
			0, uint64(sourceObject.Size))
		if err != nil {
			w.logger.Error("GetReader failed:", sourceObject.Pool,
				sourceObject.ObjectId, err.Error())
			time.Sleep(time.Second)
			continue
		}
		destCluster := w.pickSpecificCluster(backend.BIG_FILE_POOLNAME)
		if destCluster == nil {
			w.logger.Error("Empty destination cluster")
			return
		}
		newOid, bytesWritten, err := destCluster.Append(backend.BIG_FILE_POOLNAME,
			"", reader, 0, sourceObject.Size)
		if err != nil {
			w.logger.Error("cephCluster.Append error:", err, newOid)
			time.Sleep(time.Second)
			continue
		}
		if bytesWritten != uint64(sourceObject.Size) {
			_ = destCluster.Remove(backend.BIG_FILE_POOLNAME, newOid)
			w.logger.Error("cephCluster.Append write length to hdd not equal the object size:",
				newOid, bytesWritten, sourceObject.Size)
			time.Sleep(time.Second)
			continue
		}
		var newSourceObject types.Object
		newSourceObject = *sourceObject
		newSourceObject.Location = destCluster.ID()
		newSourceObject.Pool = backend.BIG_FILE_POOLNAME
		newSourceObject.ObjectId = newOid
		err = w.tikvClient.MigrateObject(&newSourceObject)
		if err != nil {
			_ = destCluster.Remove(backend.BIG_FILE_POOLNAME, newOid)
			w.logger.Error("cephCluster.Append MigrateObject failed:",
				err.Error(), newSourceObject.Pool, newSourceObject.ObjectId)
			time.Sleep(time.Second)
			continue
		}
		err = sourceCluster.Remove(backend.SMALL_FILE_POOLNAME, sourceObject.ObjectId)
		if err != nil {
			w.logger.Error("cephCluster.Append Remove data from rabbit failed:",
				err.Error(), newSourceObject.Pool, newSourceObject.ObjectId)
			return
		}
		w.InvalidCache(newSourceObject)
		w.logger.Info("migrate success for bucket: ", sourceObject.BucketName+
			" name: "+sourceObject.Name+" version: "+sourceObject.VersionId+
			" oldoid: "+sourceObject.ObjectId+" newoid: "+newSourceObject.ObjectId)
		return
	}
}

func (w MigrateWorker) migrate(messages chan types.Object,
	stopping <-chan struct{}) {

	for {
		select {
		case <-stopping:
			return
		case object := <-messages:
			w.processEntry(object)
		}
	}
}

func (w MigrateWorker) Run(messages <-chan *sarama.ConsumerMessage,
	errors <-chan *sarama.ConsumerError,
	commands <-chan string,
	stopping <-chan struct{}) {

	bufferSize := 2 * w.threadsPerWorker
	dispatchQueue := make(chan types.Object, bufferSize)
	for i := 0; i < w.threadsPerWorker; i++ {
		go w.migrate(dispatchQueue, stopping)
	}

	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				return
			}
			var object types.Object
			err := helper.MsgPackUnMarshal(msg.Value, &object)
			if err != nil {
				w.logger.Error("Unmarshal", msg.Offset, "err:", err)
				continue
			}
			dispatchQueue <- object
			if msg.Offset-int64(bufferSize) > 0 {
				err = w.handle.Save(msg.Offset-int64(bufferSize), nil)
				if err != nil {
					w.logger.Error("Save error:", err)
				}
			}
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
