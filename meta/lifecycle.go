package meta

import . "github.com/journeymidnight/yig/meta/types"

func LifeCycleFromBucket(b Bucket) (lc LifeCycle) {
	lc.BucketName = b.Name
	lc.Status = "Pending"
	return
}

func (m *Meta) PutBucketToLifeCycle(bucket Bucket) error {
	lifeCycle := LifeCycleFromBucket(bucket)
	return m.Client.PutBucketToLifeCycle(lifeCycle)
}

func (m *Meta) RemoveBucketFromLifeCycle(bucket Bucket) error {
	return m.Client.RemoveBucketFromLifeCycle(bucket)
}

func (m *Meta) ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error) {
	return m.Client.ScanLifeCycle(limit, marker)
}
