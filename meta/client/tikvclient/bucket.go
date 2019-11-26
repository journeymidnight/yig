package tikvclient

import (
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

func GenBucketKey(bucketName string) []byte {
	return GenKey(TableBucketPrefix, bucketName)
}

func (c *TiKVClient) GetBucket(bucketName string) (*Bucket, error) {
	var bucket Bucket
	b, err := c.Get(GenBucketKey(bucketName))
	if err != nil {
		return nil, err
	}
	err = helper.MsgPackUnMarshal(b, &bucket)
	if err != nil {
		return nil, err
	}
	return &bucket, nil
}

// Not ready to implement
func (c *TiKVClient) GetBuckets() (buckets []Bucket, err error) { return }

func (c *TiKVClient) PutBucket(bucket Bucket) error {
	return c.Put(GenBucketKey(bucket.Name), bucket)
}

func (c *TiKVClient) CheckAndPutBucket(bucket Bucket) (bool, error) {
	return false, nil

}
func (c *TiKVClient) DeleteBucket(bucket Bucket) error { return nil }
