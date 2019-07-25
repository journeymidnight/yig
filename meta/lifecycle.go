package meta

import (
	"context"

	. "github.com/journeymidnight/yig/meta/types"
)

func LifeCycleFromBucket(b Bucket) (lc LifeCycle) {
	lc.BucketName = b.Name
	lc.Status = "Pending"
	return
}

func (m *Meta) PutBucketToLifeCycle(bucket Bucket, ctx context.Context) error {
	lifeCycle := LifeCycleFromBucket(bucket)
	return m.Client.PutBucketToLifeCycle(lifeCycle, ctx)
}

func (m *Meta) RemoveBucketFromLifeCycle(bucket Bucket, ctx context.Context) error {
	return m.Client.RemoveBucketFromLifeCycle(bucket, ctx)
}

func (m *Meta) ScanLifeCycle(limit int, marker string, ctx context.Context) (result ScanLifeCycleResult, err error) {
	return m.Client.ScanLifeCycle(limit, marker, ctx)
}
