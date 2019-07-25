package meta

import (
	"context"

	. "github.com/journeymidnight/yig/meta/types"
)

func LifeCycleFromBucket(b *Bucket) (lc LifeCycle) {
	lc.BucketName = b.Name
	lc.Status = "Pending"
	return
}

func (m *Meta) PutBucketToLifeCycle(ctx context.Context, bucket *Bucket) error {
	lifeCycle := LifeCycleFromBucket(bucket)
	return m.Client.PutBucketToLifeCycle(lifeCycle, ctx)
}

func (m *Meta) RemoveBucketFromLifeCycle(ctx context.Context, bucket *Bucket) error {
	return m.Client.RemoveBucketFromLifeCycle(ctx, bucket)
}

func (m *Meta) ScanLifeCycle(limit int, marker string, ctx context.Context) (result ScanLifeCycleResult, err error) {
	return m.Client.ScanLifeCycle(limit, marker, ctx)
}
