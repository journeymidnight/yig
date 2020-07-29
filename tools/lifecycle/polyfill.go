package main

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/journeymidnight/yig/api/datatype"
	error2 "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/client/tikvclient"
	"github.com/journeymidnight/yig/meta/types"
	"time"
)

const (
	maxRetries   = 10
	dialTimeout  = time.Second
	readTimeout  = time.Second
	writeTimeout = time.Second
	idleTimeout  = 30 * time.Second
)

func NewRedisClient(addresses []string, password string) redis.UniversalClient {
	options := redis.UniversalOptions{
		MaxRetries:   maxRetries,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
		Addrs:        addresses,
		Password:     password,
	}
	return redis.NewUniversalClient(&options)
}

func (w LifecycleWorker) addDeleteMarker(bucket, object, ownerID string,
	nullVersionMarker bool) error {

	now := time.Now()
	marker := &types.Object{
		BucketName:       bucket,
		Name:             object,
		OwnerId:          ownerID,
		DeleteMarker:     true,
		LastModifiedTime: now.UTC(),
		CreateTime:       uint64(now.UnixNano()),
		Size:             int64(len(object)),
	}
	if nullVersionMarker {
		marker.VersionId = types.NullVersion
	} else {
		marker.VersionId = marker.GenVersionId(datatype.BucketVersioningEnabled)
	}
	err := w.tikvClient.PutObject(marker, nil, true)
	if err != nil {
		return fmt.Errorf("put delete marker: %w", err)
	}
	return nil
}

// remove object/part, and send a message to gc
func (w LifecycleWorker) gcObject(object types.Object, tx driver.Tx) error {
	err := w.tikvClient.DeleteObject(&object, tx)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	garbageObject := types.GetGcInfoFromObject(&object)
	v, err := helper.MsgPackMarshal(garbageObject)
	if err != nil {
		return err
	}
	w.GarbageCollectionProducer.Publish("", v)
	return nil
}

func (w LifecycleWorker) deleteObject(object types.Object) (deltaSize int64, err error) {
	bucket, err := w.tikvClient.GetBucket(object.BucketName)
	if err != nil {
		return 0, fmt.Errorf("get bucket: %w", err)
	}
	switch bucket.Versioning {
	case datatype.BucketVersioningDisabled:
		object, err := w.tikvClient.GetObject(object.BucketName, object.Name,
			types.NullVersion, nil)
		if err == error2.ErrNoSuchKey {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		err = w.gcObject(*object, nil)
		if err != nil {
			return 0, err
		}
		deltaSize = -object.Size
	case datatype.BucketVersioningEnabled:
		err = w.addDeleteMarker(object.BucketName, object.Name,
			bucket.OwnerId, false)
		if err != nil {
			return 0, err
		}
		deltaSize = int64(len(object.Name))
	case datatype.BucketVersioningSuspended:
		tx, err := w.tikvClient.NewTrans()
		if err != nil {
			return 0, err
		}
		// TODO tx
		object, err := w.tikvClient.GetObject(object.BucketName, object.Name,
			types.NullVersion, nil)
		if err != nil && err != error2.ErrNoSuchKey {
			return 0, err
		}
		if err != error2.ErrNoSuchKey {
			err = w.gcObject(*object, tx)
			if err != nil {
				return 0, err
			}
		}
		err = w.addDeleteMarker(object.BucketName, object.Name,
			bucket.OwnerId, true)
		if err != nil {
			return 0, err
		}
	}
}
