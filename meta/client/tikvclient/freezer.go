package tikvclient

import (
	"context"
	. "database/sql/driver"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/tikv/client-go/txnkv/kv"
)

func genFreezerKey(bucketName, objectName, version string) []byte {
	if version == NullVersion {
		return GenKey(TableFreezerPrefix, bucketName, objectName)
	} else {
		return GenKey(TableFreezerPrefix, bucketName, objectName, version)
	}
}

//freezer
func (c *TiKVClient) CreateFreezer(freezer *Freezer) (err error) {
	key := genFreezerKey(freezer.BucketName, freezer.Name, freezer.VersionId)
	return c.TxPut(key, *freezer)
}

func (c *TiKVClient) GetFreezer(bucketName, objectName, version string) (freezer *Freezer, err error) {
	key := genFreezerKey(bucketName, objectName, version)
	var f Freezer
	ok, err := c.TxGet(key, &f, nil)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNoSuchKey
	}

	if f.Parts != nil && len(f.Parts) != 0 {
		var sortedPartNum = make([]int64, len(f.Parts))
		for k, v := range f.Parts {
			sortedPartNum[k-1] = v.Offset
		}
		f.PartsIndex = &SimpleIndex{Index: sortedPartNum}
	}
	return &f, nil
}

func (c *TiKVClient) GetFreezerStatus(bucketName, objectName, version string) (freezer *Freezer, err error) {
	key := genFreezerKey(bucketName, objectName, version)
	var f Freezer
	ok, err := c.TxGet(key, &f, nil)
	if err != nil {
		return nil, err
	}
	if !ok {
		return &f, ErrNoSuchKey
	}
	return &f, nil
}

func (c *TiKVClient) UpdateFreezerDate(bucketName, objectName, version string, lifetime int) (err error) {
	key := genFreezerKey(bucketName, objectName, version)
	tx, err := c.NewTrans()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = c.CommitTrans(tx)
		}
		if err != nil {
			c.AbortTrans(tx)
		}
	}()

	txn := tx.(*TikvTx).tx
	val, err := txn.Get(context.TODO(), key)
	if err != nil && !kv.IsErrNotFound(err) {
		return err
	}
	if kv.IsErrNotFound(err) {
		return nil
	}
	var f Freezer
	err = helper.MsgPackUnMarshal(val, &f)
	if err != nil {
		return err
	}

	f.LifeTime = lifetime

	newVal, err := helper.MsgPackMarshal(f)
	if err != nil {
		return err
	}

	err = txn.Set(key, newVal)
	if err != nil {
		return err
	}
	return nil
}

func (c *TiKVClient) DeleteFreezer(bucketName, objectName, versionId string, objectType ObjectType, createTime uint64, tx Tx) (err error) {
	key := genFreezerKey(bucketName, objectName, versionId)
	return c.TxDelete(key)
}
