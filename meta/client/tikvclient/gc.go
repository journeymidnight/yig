package tikvclient

import (
	. "database/sql/driver"
	"errors"
	"strings"

	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

// **Key**: g\{PoolName}\{Fsid}\{ObjectId}
func genGcKey(fsid, poolName, objectId string) []byte {
	return GenKey(TableGcPrefix, poolName, fsid, objectId)
}

//gc
func (c *TiKVClient) PutObjectToGarbageCollection(object *Object, tx Tx) error {
	key := genGcKey(object.Location, object.Pool, object.ObjectId)
	gc := GetGcInfoFromObject(object)
	if tx == nil {
		return c.TxPut(key, gc)
	}
	txn := tx.(*TikvTx).tx
	v, err := helper.MsgPackMarshal(gc)
	if err != nil {
		return err
	}
	return txn.Set(key, v)
}

func (c *TiKVClient) ScanGarbageCollection(limit int) (gcs []GarbageCollection, err error) {
	startKey := GenKey(TableGcPrefix, TableMinKeySuffix)
	endKey := GenKey(TableGcPrefix, TableMaxKeySuffix)
	kvs, err := c.TxScan(startKey, endKey, limit)
	if err != nil {
		return nil, err
	}

	for _, kv := range kvs {
		var gc GarbageCollection
		key, val := kv.K, kv.V
		err = helper.MsgPackUnMarshal(val, &gc)
		sp := strings.Split(string(key), TableSeparator)
		if len(sp) != 4 {
			return nil, errors.New("Invalid gc key:" + string(key))
		}
		gcs = append(gcs, gc)
	}
	return
}

func (c *TiKVClient) RemoveGarbageCollection(garbage GarbageCollection) error {
	key := genGcKey(garbage.Location, garbage.Pool, garbage.ObjectId)
	return c.TxDelete(key)
}

func (c *TiKVClient) PutFreezerToGarbageCollection(f *Freezer, tx Tx) (err error) {
	key := genGcKey(f.Location, f.Pool, f.ObjectId)
	object := f.ToObject()
	gc := GetGcInfoFromObject(&object)
	if tx == nil {
		return c.TxPut(key, gc)
	}
	txn := tx.(*TikvTx).tx
	v, err := helper.MsgPackMarshal(gc)
	if err != nil {
		return err
	}
	return txn.Set(key, v)
}
