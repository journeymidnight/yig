package tikvclient

import (
	. "github.com/journeymidnight/yig/meta/types"
)

func genLifecycleKey() []byte {
	return GenKey(TableLifeCyclePrefix)
}

//lc
func (c *TiKVClient) PutBucketToLifeCycle(lifeCycle LifeCycle) error { return nil }
func (c *TiKVClient) RemoveBucketFromLifeCycle(bucket Bucket) error  { return nil }
func (c *TiKVClient) ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error) {
	return
}
