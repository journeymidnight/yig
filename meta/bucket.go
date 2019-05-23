package meta

import (
	"fmt"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
)

const (
	BUCKET_USAGE_CACHE_PREFIX = "bucket_usage_"
)

// Note the usage info got from this method is possibly not accurate because we don't
// invalid cache when updating usage. For accurate usage info, use `GetUsage()`
func (m *Meta) GetBucket(bucketName string, willNeed bool) (bucket Bucket, err error) {
	getBucket := func() (b interface{}, err error) {
		b, err = m.Client.GetBucket(bucketName)
		helper.Logger.Println(10, "GetBucket CacheMiss. bucket:", bucketName)
		return b, err
	}

	b, err := m.Cache.Get(redis.BucketTable, bucketName, getBucket, willNeed)
	if err != nil {
		return
	}
	bucket, ok := b.(Bucket)
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

func (m *Meta) UpdateUsage(bucketName string, size int64) error {
	usage, err := m.Cache.IncrBy(redis.BucketTable, BUCKET_USAGE_CACHE_PREFIX+bucketName, size)
	if err != nil {
		return err
	}
	helper.Logger.Println(15, "incr usage for bucket: ", bucketName, ", updated to ", usage)
	return nil
}

func (m *Meta) GetUsage(bucketName string) (int64, error) {
	usage, err := m.Cache.Get(redis.BucketTable, BUCKET_USAGE_CACHE_PREFIX+bucketName, nil, false)
	if err != nil {
		helper.Logger.Println(2, "failed to get usage for bucket: ", bucketName, ", err: ", err)
		return 0, err
	}
	return usage.(int64), nil
}

func (m *Meta) GetBucketInfo(bucketName string) (Bucket, error) {
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

/*
* init bucket usage cache when meta is newed.
*
 */
func (m *Meta) InitBucketUsageCache() error {
	// the map contains the bucket usage which are not in cache.
	bucketUsageMap := make(map[string]interface{})
	// the map contains the bucket usage which are in cache and will be synced into database.
	bucketUsageCacheMap := make(map[string]int64)
	// the usage in buckets table is accurate now.
	buckets, err := m.Client.GetBuckets()
	if err != nil {
		helper.Logger.Println(2, "failed to get buckets from db. err: ", err)
		return err
	}

	// init the bucket usage key in cache.
	for _, bucket := range buckets {
		key := fmt.Sprintf("%s%s", BUCKET_USAGE_CACHE_PREFIX, bucket)
		bucketUsageMap[key] = bucket.Usage
	}

	// try to get all bucket usage keys from cache.
	pattern := fmt.Sprintf("%s*", BUCKET_USAGE_CACHE_PREFIX)
	bucketsInCache, err := m.Cache.Keys(redis.BucketTable, pattern)
	if err != nil {
		helper.Logger.Println(2, "failed to get bucket usage from cache, err: ", err)
		return err
	}

	// query all usages from cache.
	usages, err := m.Cache.MGet(redis.BucketTable, bucketsInCache)
	if err != nil {
		helper.Logger.Println(2, "failed to get usages for existing buckets, err: ", err)
		return err
	}

	for i, bc := range bucketsInCache {
		_, ok := bucketUsageMap[bc]
		// if the key already exists in cache, then delete it from map
		if ok {
			// add the to be synced usage.
			bucketUsageCacheMap[bc] = usages[i].(int64)
			delete(bucketUsageMap, bc)
		}
	}

	// init the bucket usage in cache.
	result, err := m.Cache.MSet(redis.BucketTable, bucketUsageMap)
	if err != nil {
		helper.Logger.Println(2, "failed to call mset for bucket usage map, result: ", result, ", err: ", err)
		return err
	}
	// sync the buckets usage in cache into database.
	err = m.Client.UpdateUsages(bucketUsageCacheMap, nil)
	if err != nil {
		helper.Logger.Println(2, "failed to sync usages to database, err: ", err)
		return err
	}
	return nil
}

func (m *Meta) bucketUsageSync(bucketName string) error {
	result, err := m.Cache.Get(redis.BucketTable, BUCKET_USAGE_CACHE_PREFIX+bucketName, nil, false)
	if err != nil {
		helper.Logger.Println(2, "failed to query bucket usage for ", bucketName, " from cache, err: ", err)
		return err
	}

	err = m.Client.UpdateUsage(bucketName, result.(int64), nil)
	if err != nil {
		helper.Logger.Println(2, "failed to update bucket usage ", result.(int64), " to bucket: ", bucketName,
			" err: ", err)
		return err
	}

	helper.Logger.Println(15, "succeed to update bucket usage ", result.(int64), " for bucket: ", bucketName)
	return nil
}
