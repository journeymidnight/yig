package main

import (
	"10.0.45.221/meepo/log.git"
	"10.0.45.221/meepo/task.git"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/journeymidnight/yig/backend"
	"github.com/journeymidnight/yig/ceph"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/types"
	"strings"
	"time"
)

func TaskObjects() []interface{} {
	return []interface{}{
		AsyncDeleteFromCeph{},
	}
}

/*
When Yig deletes a object, it would send a Kafka message.
This task consumes those messages and actually delete them in Ceph.
*/
type AsyncDeleteFromCeph struct {
	logger         log.Logger
	handle         task.Handle
	cephClusters   map[string]backend.Cluster
	logDeleteFiles bool
	logMessage     bool
}

func (d AsyncDeleteFromCeph) Name() string {
	return "yig_delete"
}

type Conf struct {
	GarbageCollectionTopic string
	PartitionCount         int
	CephConfigPattern      string
	LogDeleteFiles         bool
	LogMessage             bool
}

func (d AsyncDeleteFromCeph) Setup(handle task.ConfigHandle) (topic string,
	partitions int, err error) {

	var conf Conf
	err = handle.ReadConfig(&conf)
	if err != nil {
		return "", 0, err
	}
	return conf.GarbageCollectionTopic, conf.PartitionCount, nil
}

func (d AsyncDeleteFromCeph) Init(handle task.Handle,
	jobID int) (task.AsyncTask, error) {

	var conf Conf
	err := handle.ReadConfig(&conf)
	if err != nil {
		return nil, err
	}
	cephClusters, err := ceph.PureInitialize(conf.CephConfigPattern)
	if err != nil {
		return nil, err
	}
	return AsyncDeleteFromCeph{
		logger:         handle.GetLogger(),
		handle:         handle,
		cephClusters:   cephClusters,
		logDeleteFiles: conf.LogDeleteFiles,
		logMessage:     conf.LogMessage,
	}, nil
}

func (d AsyncDeleteFromCeph) ensureRemoved(location, pool, cephObjectID string) {
	for {
		err := d.cephClusters[location].Remove(pool, cephObjectID)
		if err == nil {
			return
		}
		// 2 is ENOENT, "no such file or directory",
		// meaning the object may already be deleted
		if strings.Contains(err.Error(), "ret=-2") {
			return
		}
		// 34 is ERANGE, meaning the striper layout has been changed,
		// might need to delete it manually
		if strings.Contains(err.Error(), "ret=-34") {
			d.logger.Error("Object", location, pool, cephObjectID,
				"is of different striper layout, need to be deleted manually")
			return
		}
		d.logger.Warn("Delete", location, pool, cephObjectID,
			"failed:", err)
		time.Sleep(time.Second)
	}
}

func (d AsyncDeleteFromCeph) handleMessage(msg *sarama.ConsumerMessage) {
	var garbage types.GarbageCollection
	err := helper.MsgPackUnMarshal(msg.Value, &garbage)
	if err != nil {
		d.logger.Warn("Bad message:", msg.Offset, err)
		return
	}
	if d.logMessage {
		d.logger.Info("Unmarshalled message:",
			fmt.Sprintf("%+v", garbage))
	}
	if _, ok := d.cephClusters[garbage.Location]; !ok {
		d.logger.Warn("Bad garbage location:", garbage.Location, msg.Offset)
		return
	}
	if d.logDeleteFiles {
		defer func() {
			d.logger.Info("Deleted:",
				garbage.BucketName, garbage.ObjectName, garbage.VersionId,
				garbage.Location, garbage.Pool, garbage.ObjectId)
		}()
	}
	if len(garbage.Parts) == 0 {
		d.ensureRemoved(garbage.Location, garbage.Pool, garbage.ObjectId)
		return
	}
	for _, p := range garbage.Parts {
		d.ensureRemoved(garbage.Location, garbage.Pool, p.ObjectId)
	}
}

func (d AsyncDeleteFromCeph) Run(
	messages <-chan *sarama.ConsumerMessage,
	errors <-chan *sarama.ConsumerError,
	commands <-chan string,
	stopping <-chan struct{}) {

	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				return
			}
			d.handleMessage(msg)
			_ = d.handle.Save(msg.Offset, nil)
		case err, ok := <-errors:
			if !ok {
				return
			}
			d.logger.Error("Consume error:", err)
		case _, ok := <-commands:
			if !ok {
				return
			}
		case <-stopping:
			for _, cluster := range d.cephClusters {
				cluster.Close()
			}
			return
		}
	}
}
