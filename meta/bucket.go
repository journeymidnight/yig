package meta

import (
	"fmt"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
)

const (
	BUCKET_CACHE_PREFIX = "bucket:"
	USER_CACHE_PREFIX   = "user:"
)

// Note the usage info got from this method is possibly not accurate because we don't
// invalid cache when updating usage. For accurate usage info, use `GetUsage()`
func (m *Meta) GetBucket(bucketName string, willNeed bool) (bucket *Bucket, err error) {
	getBucket := func() (b helper.Serializable, err error) {
		bt, err := m.Client.GetBucket(bucketName)
		helper.Logger.Println(10, "GetBucket CacheMiss. bucket:", bucketName)
		return bt, err
	}

	toBucket := func(fields map[string]string) (interface{}, error) {
		b := &Bucket{}
		return b.Deserialize(fields)
	}

	b, err := m.Cache.Get(redis.BucketTable, BUCKET_CACHE_PREFIX, bucketName, getBucket, toBucket, willNeed)
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

func (m *Meta) GetBuckets() (buckets []*Bucket, err error) {
	buckets, err = m.Client.GetBuckets()
	return
}

func (m *Meta) UpdateUsage(bucketName string, size int64) error {
	usage, err := m.Cache.HIncrBy(redis.BucketTable, BUCKET_CACHE_PREFIX, bucketName, FIELD_NAME_USAGE, size)
	if err != nil {
		helper.Logger.Println(2, fmt.Sprintf("failed to update bucket[%s] usage by %d, err: %v",
			bucketName, size, err))
		return err
	}

	AddBucketUsageSyncEvent(bucketName, usage)
	helper.Logger.Println(15, "incr usage for bucket: ", bucketName, ", updated to ", usage)
	return nil
}

func (m *Meta) GetUsage(bucketName string) (int64, error) {
	usage, err := m.Cache.HGetInt64(redis.BucketTable, BUCKET_CACHE_PREFIX, bucketName, FIELD_NAME_USAGE)
	if err != nil {
		helper.Logger.Println(2, "failed to get usage for bucket: ", bucketName, ", err: ", err)
		return 0, err
	}
	return usage, nil
}

func (m *Meta) GetBucketInfo(bucketName string) (*Bucket, error) {
	m.Cache.Remove(redis.BucketTable, BUCKET_CACHE_PREFIX, bucketName)
	bucket, err := m.GetBucket(bucketName, true)
	if err != nil {
		return bucket, err
	}
	return bucket, nil
}

func (m *Meta) GetUserInfo(uid string) ([]string, error) {
	m.Cache.Remove(redis.UserTable, USER_CACHE_PREFIX, uid)
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
	bucketUsageMap := make(map[string]*Bucket)
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
		bucketUsageMap[bucket.Name] = bucket
	}

	// try to get all bucket usage keys from cache.
	pattern := fmt.Sprintf("%s*", BUCKET_CACHE_PREFIX)
	bucketsInCache, err := m.Cache.Keys(redis.BucketTable, pattern)
	if err != nil {
		helper.Logger.Println(2, "failed to get bucket usage from cache, err: ", err)
		return err
	}

	if len(bucketsInCache) > 0 {
		// query all usages from cache.
		for _, bic := range bucketsInCache {
			usage, err := m.Cache.HGetInt64(redis.BucketTable, BUCKET_CACHE_PREFIX, bic, FIELD_NAME_USAGE)
			if err != nil {
				helper.Logger.Println(2, "failed to get usage for bucket: ", bic, " with err: ", err)
				continue
			}
			// add the to be synced usage.
			bucketUsageCacheMap[bic] = usage
			if _, ok := bucketUsageMap[bic]; ok {
				// if the key already exists in cache, then delete it from map
				delete(bucketUsageMap, bic)
			}
		}

	}

	// init the bucket usage in cache.
	if len(bucketUsageMap) > 0 {
		for _, bk := range bucketUsageMap {
			fields, err := bk.Serialize()
			if err != nil {
				helper.Logger.Println(2, "failed to serialize for bucket: ", bk.Name, " with err: ", err)
				return err
			}
			_, err = m.Cache.HMSet(redis.BucketTable, BUCKET_CACHE_PREFIX, bk.Name, fields)
			if err != nil {
				helper.Logger.Println(2, "failed to set bucket to cache: ", bk.Name, " with err: ", err)
				return err
			}
		}

	}
	// sync the buckets usage in cache into database.
	if len(bucketUsageCacheMap) > 0 {
		err = m.Client.UpdateUsages(bucketUsageCacheMap, nil)
		if err != nil {
			helper.Logger.Println(2, "failed to sync usages to database, err: ", err)
			return err
		}
	}
	return nil
}

func (m *Meta) bucketUsageSync(event SyncEvent) error {
	bu := &BucketUsageEvent{}
	err := helper.MsgPackUnMarshal(event.Data.([]byte), bu)
	if err != nil {
		helper.Logger.Println(2, "failed to unpack from event data to BucketUsageEvent, err: %v", err)
		return err
	}

	err = m.Client.UpdateUsage(bu.BucketName, bu.Usage, nil)
	if err != nil {
		helper.Logger.Println(2, "failed to update bucket usage ", bu.Usage, " to bucket: ", bu.BucketName,
			" err: ", err)
		return err
	}

	helper.Logger.Println(15, "succeed to update bucket usage ", bu.Usage, " for bucket: ", bu.BucketName)
	return nil
}

func AddBucketUsageSyncEvent(bucketName string, usage int64) {
	bu := &BucketUsageEvent{
		Usage:      usage,
		BucketName: bucketName,
	}
	data, err := helper.MsgPackMarshal(bu)
	if err != nil {
		helper.Logger.Printf(2, "failed to package bucket usage event for bucket %s with usage %d, err: %v",
			bucketName, usage, err)
		return
	}
	if MetaSyncQueue != nil {
		event := SyncEvent{
			Type: SYNC_EVENT_TYPE_BUCKET_USAGE,
			Data: data,
		}
		MetaSyncQueue <- event
	}
}
