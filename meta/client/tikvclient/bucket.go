package tikvclient

import (
	"math"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

func genBucketKey(bucketName string) []byte {
	return GenKey(false, TableBucketPrefix, bucketName)
}

func genUserBucketKey(ownerId, bucketName string) []byte {
	return GenKey(false, TableUserBucketPrefix, ownerId, bucketName)
}

//bucket
func (c *TiKVClient) GetBucket(bucketName string) (*Bucket, error) {
	key := genBucketKey(bucketName)
	v, err := c.Get(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrNoSuchBucket
	}
	var b Bucket
	err = helper.MsgPackUnMarshal(v, &b)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// TODO: To be deprecated
func (c *TiKVClient) GetBuckets() (buckets []Bucket, err error) {
	startKey := GenKey(true, TableBucketPrefix)
	endKey := GenKey(false, TableBucketPrefix, TableMaxKeySuffix)
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

func (c *TiKVClient) UpdateUsage(bucketName string, size int64, _ DB) error {
	// TODO: TBD
	return nil
}

func (c *TiKVClient) IsEmptyBucket(bucketName string) (isEmpty bool, err error) {
	bucketStartKey := GenKey(true, bucketName)
	bucketEndKey := GenKey(false, bucketName, TableMaxKeySuffix)
	partStartKey := GenKey(true, TableObjectPartPrefix, bucketName)
	partEndKey := GenKey(false, bucketName, TableMaxKeySuffix)
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
