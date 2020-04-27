package meta

import (
	. "github.com/journeymidnight/yig/meta/types"
	"time"
)

func (m *Meta) PutBucketToLifeCycle(bucket Bucket) error {
	lifeCycle := LifeCycle{
		BucketName: bucket.Name,
		Status:     LcPending,
		StartTime:  uint64(time.Now().Local().UnixNano()),
		EndTime:    uint64(time.Now().Local().UnixNano()),
	}
	return m.Client.PutBucketToLifeCycle(lifeCycle)
}

func (m *Meta) GetBucketLifeCycle(bucket Bucket) (lifeCycle *LifeCycle, err error) {
	return m.Client.GetBucketLifeCycle(bucket)
}

func (m *Meta) RemoveBucketFromLifeCycle(bucket Bucket) error {
	return m.Client.RemoveBucketFromLifeCycle(bucket)
}

func (m *Meta) ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error) {
	return m.Client.ScanLifeCycle(limit, marker)
}
