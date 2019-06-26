package meta

import (
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
)

// Note the usage info got from this method is possibly not accurate because we don't
// invalid cache when updating usage. For accurate usage info, use `GetUsage()`
func (m *Meta) GetBucket(bucketName string, willNeed bool) (bucket *Bucket, err error) {
	getBucket := func() (b interface{}, err error) {
		b, err = m.Client.GetBucket(bucketName)
		helper.Logger.Println(10, "GetBucket CacheMiss. bucket:", bucketName)
		return b, err
	}
	unmarshaller := func(in []byte) (interface{}, error) {
		var bucket Bucket
		err := helper.MsgPackUnMarshal(in, &bucket)
		return &bucket, err
	}
	b, err := m.Cache.Get(redis.BucketTable, bucketName, getBucket, unmarshaller, willNeed)
	if err != nil {
		return
	}
	bucket, ok := b.(*Bucket)
	if !ok {
		helper.Debugln("Cast b failed:", b)
		err = ErrInternalError
		return
	}
	return bucket, nil
}

func (m *Meta) GetBuckets() (buckets []Bucket, err error) {
	buckets, err = m.Client.GetBuckets()
	return
}

func (m *Meta) UpdateUsage(bucketName string, size int64) {
	m.Client.UpdateUsage(bucketName, size, nil)
}

func (m *Meta) GetUsage(bucketName string) (int64, error) {
	m.Cache.Remove(redis.BucketTable, bucketName)
	bucket, err := m.GetBucket(bucketName, true)
	if err != nil {
		return 0, err
	}
	return bucket.Usage, nil
}

func (m *Meta) GetBucketInfo(bucketName string) (*Bucket, error) {
	m.Cache.Remove(redis.BucketTable, bucketName)
	bucket, err := m.GetBucket(bucketName, true)
	if err != nil {
		return bucket, err
	}
	return bucket, nil
}

func (m *Meta) GetUserInfo(uid string) ([]string, error) {
	m.Cache.Remove(redis.UserTable, uid)
	buckets, err := m.GetUserBuckets(uid, true)
	if err != nil {
		return nil, err
	}
	return buckets, nil
}
