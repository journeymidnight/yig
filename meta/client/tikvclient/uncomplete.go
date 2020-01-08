package tikvclient

import (
	. "github.com/journeymidnight/yig/meta/types"
)

//user
func (c *TiKVClient) GetUserBuckets(userId string) (buckets []string, err error) { return nil, nil }

//gc
func (c *TiKVClient) PutObjectToGarbageCollection(object *Object, tx DB) error { return nil }
func (c *TiKVClient) ScanGarbageCollection(limit int, startRowKey string) ([]GarbageCollection, error) {
	return nil, nil
}
func (c *TiKVClient) RemoveGarbageCollection(garbage GarbageCollection) error { return nil }
