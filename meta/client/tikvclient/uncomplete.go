package tikvclient

import (
	. "database/sql/driver"

	. "github.com/journeymidnight/yig/meta/types"
)

//gc
func (c *TiKVClient) PutObjectToGarbageCollection(object *Object, tx Tx) error { return nil }
func (c *TiKVClient) ScanGarbageCollection(limit int, startRowKey string) ([]GarbageCollection, error) {
	return nil, nil
}
func (c *TiKVClient) RemoveGarbageCollection(garbage GarbageCollection) error { return nil }
