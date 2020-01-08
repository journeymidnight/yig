package tikvclient

import (
	"context"
	. "database/sql/driver"
	"math"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

func genBucketKey(bucketName string) []byte {
	return GenKey(TableBucketPrefix, bucketName)
}

//bucket
func (c *TiKVClient) GetBucket(bucketName string) (*Bucket, error) {
	key := genBucketKey(bucketName)
	var b Bucket
	ok, err := c.Get(key, &b)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNoSuchBucket
	}
	return &b, nil
}

// TODO: To be deprecated
func (c *TiKVClient) GetBuckets() (buckets []Bucket, err error) {
	startKey := GenKey(TableBucketPrefix, TableMinKeySuffix)
	endKey := GenKey(TableBucketPrefix, TableMaxKeySuffix)
	kvs, err := c.Scan(startKey, endKey, math.MaxUint64)
	for _, kv := range kvs {
		var b Bucket
		err = helper.MsgPackUnMarshal(kv.V, &b)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, b)
	}
	return buckets, nil
}

func (c *TiKVClient) PutBucket(bucket Bucket) error {
	key := genBucketKey(bucket.Name)
	return c.Put(key, bucket)
}

func (c *TiKVClient) PutNewBucket(bucket Bucket) error {
	bucketKey := genBucketKey(bucket.Name)
	userBucketKey := genUserBucketKey(bucket.OwnerId, bucket.Name)
	return c.TxPut(bucketKey, bucketKey, userBucketKey, 0)
}

func (c *TiKVClient) DeleteBucket(bucket Bucket) error {
	bucketKey := genBucketKey(bucket.Name)
	userBucketKey := genUserBucketKey(bucket.OwnerId, bucket.Name)
	lifeCycleKey := genLifecycleKey()
	return c.TxDelete(bucketKey, userBucketKey, lifeCycleKey)
}

func (c *TiKVClient) ListObjects(bucketName, marker, verIdMarker, prefix, delimiter string, versioned bool,
	maxKeys int) (retObjects []*Object, prefixes []string, truncated bool, nextMarker, nextVerIdMarker string, err error) {
	return
}

func (c *TiKVClient) UpdateUsage(bucketName string, size int64, tx Tx) error {
	if !helper.CONFIG.PiggybackUpdateUsage {
		return nil
	}

	bucket, err := c.GetBucket(bucketName)
	if err != nil {
		return err
	}

	userBucketKey := genUserBucketKey(bucket.OwnerId, bucket.Name)
	var usage int64

	if tx == nil {
		ok, err := c.Get(userBucketKey, &usage)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNoSuchBucket
		}
		usage += size
		return c.Put(userBucketKey, usage)
	}

	v, err := tx.(*TikvTx).tx.Get(context.TODO(), userBucketKey)
	if err != nil {
		return err
	}

	err = helper.MsgPackUnMarshal(v, &usage)
	if err != nil {
		return err
	}

	usage += size

	v, err = helper.MsgPackMarshal(usage)
	if err != nil {
		return err
	}
	return tx.(*TikvTx).tx.Set(userBucketKey, v)
}

func (c *TiKVClient) IsEmptyBucket(bucketName string) (isEmpty bool, err error) {
	bucketStartKey := GenKey(bucketName, TableMinKeySuffix)
	bucketEndKey := GenKey(bucketName, TableMaxKeySuffix)
	partStartKey := GenKey(TableObjectPartPrefix, bucketName, TableMinKeySuffix)
	partEndKey := GenKey(TableObjectPartPrefix, bucketName, TableMaxKeySuffix)
	r, err := c.Scan(bucketStartKey, bucketEndKey, 1)
	if err != nil {
		return false, err
	}
	if len(r) > 0 {
		return false, nil
	}
	r, err = c.Scan(partStartKey, partEndKey, 1)
	if err != nil {
		return false, err
	}
	if len(r) > 0 {
		return false, nil
	}
	return true, nil

}
