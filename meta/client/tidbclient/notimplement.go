package tidbclient

import (
	. "github.com/journeymidnight/yig/error"
	. "github.com/journeymidnight/yig/meta/types"
)

//lc
func (t *TidbClient) PutBucketToLifeCycle(lifeCycle LifeCycle) error {
	return ErrNotImplemented
}

func (t *TidbClient) RemoveBucketFromLifeCycle(bucket Bucket) error {
	return ErrNotImplemented
}

func (t *TidbClient) ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error) {
	return
}
