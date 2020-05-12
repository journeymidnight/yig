package tikvclient

import (
	. "github.com/journeymidnight/yig/error"
	. "github.com/journeymidnight/yig/meta/types"
)

// **Key**: {BucketName}\{ObjectName}
func genLifecycleKey(bucketName string) []byte {
	return GenKey(TableLifeCyclePrefix, bucketName)
}

//lc
func (c *TiKVClient) PutBucketToLifeCycle(lifeCycle LifeCycle) error {

	return nil
}

func (c *TiKVClient) GetBucketLifeCycle(bucket Bucket) (*LifeCycle, error) {
	key := genLifecycleKey(bucket.Name)
	var lc LifeCycle
	ok, err := c.TxGet(key, &lc)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNoSuchKey
	}
	return &lc, nil
}

func (c *TiKVClient) RemoveBucketFromLifeCycle(bucket Bucket) error {
	return nil
}

func (c *TiKVClient) ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error) {
	return
}
