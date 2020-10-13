package tikvclient

import (
	"context"
	. "database/sql/driver"

	"github.com/journeymidnight/client-go/key"

	"github.com/journeymidnight/yig/meta/common"

	"github.com/journeymidnight/client-go/txnkv/kv"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

func GenFreezerKey(bucketName, objectName, version string) []byte {
	if version == NullVersion {
		return GenKey(TableFreezerPrefix, bucketName, objectName)
	} else {
		return GenKey(TableFreezerPrefix, bucketName, objectName, version)
	}
}

//freezer
func (c *TiKVClient) CreateFreezer(freezer *Freezer) (err error) {
	key := GenFreezerKey(freezer.BucketName, freezer.Name, freezer.VersionId)
	err = c.TxPut(key, *freezer)
	if err != nil {
		return NewError(InTikvFatalError, "CreateFreezer TxPut err", err)
	}
	return nil
}

func (c *TiKVClient) GetFreezer(bucketName, objectName, version string) (freezer *Freezer, err error) {
	key := GenFreezerKey(bucketName, objectName, version)
	var f Freezer
	ok, err := c.TxGet(key, &f, nil)
	if err != nil {
		err := NewError(InTikvFatalError, "GetFreezer TxGet err", err)
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
	key := GenFreezerKey(bucketName, objectName, version)
	var f Freezer
	ok, err := c.TxGet(key, &f, nil)
	if err != nil {
		err := NewError(InTikvFatalError, "GetFreezerStatus TxGet err", err)
		return nil, err
	}
	if !ok {
		return &f, ErrNoSuchKey
	}
	return &f, nil
}

func (c *TiKVClient) UpdateFreezerDate(bucketName, objectName, version string, lifetime int) (err error) {
	key := GenFreezerKey(bucketName, objectName, version)
	tx, err := c.NewTrans()
	if err != nil {
		return NewError(InTikvFatalError, "UpdateFreezerDate NewTrans err", err)
	}
	defer func() {
		if err == nil {
			err = c.CommitTrans(tx)
			if err != nil {
				err = NewError(InTikvFatalError, "UpdateFreezerDate err", err)
			}
		}
		if err != nil {
			c.AbortTrans(tx)
		}
	}()

	txn := tx.(*TikvTx).tx
	val, err := txn.Get(context.TODO(), key)
	if err != nil && !kv.IsErrNotFound(err) {
		return NewError(InTikvFatalError, "UpdateFreezerDate Get err", err)
	}
	if kv.IsErrNotFound(err) {
		return nil
	}
	var f Freezer
	err = helper.MsgPackUnMarshal(val, &f)
	if err != nil {
		return NewError(InTikvFatalError, "UpdateFreezerDate MsgPackUnMarshal err", err)
	}

	f.LifeTime = lifetime

	newVal, err := helper.MsgPackMarshal(f)
	if err != nil {
		return NewError(InTikvFatalError, "UpdateFreezerDate MsgPackMarshal err", err)
	}

	err = txn.Set(key, newVal)
	if err != nil {
		return NewError(InTikvFatalError, "UpdateFreezerDate Set err", err)
	}
	return nil
}

func (c *TiKVClient) DeleteFreezer(bucketName, objectName, versionId string, objectType ObjectType, createTime uint64, tx Tx) (err error) {
	key := GenFreezerKey(bucketName, objectName, versionId)
	err = c.TxDelete(key)
	if err != nil {
		return NewError(InTikvFatalError, "DeleteFreezer TxDelete err", err)
	}
	return nil
}

func (c *TiKVClient) ListFreezersWithStatus(maxKeys int, status common.RestoreStatus) (retFreezers []Freezer, err error) {
	startKey := GenFreezerKey(TableMinKeySuffix, TableMinKeySuffix, NullVersion)
	endKey := GenFreezerKey(TableMaxKeySuffix, TableMaxKeySuffix, TableMaxKeySuffix)
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		err = NewError(InTikvFatalError, "ListFreezersWithStatus Begin err", err)
		return nil, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		err = NewError(InTikvFatalError, "ListFreezersWithStatus Iter err", err)
		return nil, err
	}
	defer it.Close()
	for it.Valid() {
		v := it.Value()
		var f Freezer
		err = helper.MsgPackUnMarshal(v, &f)
		if err != nil {
			err = NewError(InTikvFatalError, "ListFreezersWithStatus MsgPackUnMarshal err", err)
			return nil, err
		}
		if f.Status.ToString() != status.ToString() {
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				err = NewError(InTikvFatalError, "ListFreezersWithStatus get next err", err)
				return nil, err
			}
			continue
		}
		retFreezers = append(retFreezers, f)
		if maxKeys != -1 {
			maxKeys--
			if maxKeys == 0 {
				break
			}
		}
		if err := it.Next(context.TODO()); err != nil && it.Valid() {
			err = NewError(InTikvFatalError, "ListFreezersWithStatus get next err", err)
			return nil, err
		}
	}
	return
}

func (c *TiKVClient) PutFreezer(freezer *Freezer, status common.RestoreStatus, tx Tx) (err error) {
	key := GenFreezerKey(freezer.BucketName, freezer.Name, freezer.VersionId)
	if tx == nil {
		tx, err = c.NewTrans()
		if err != nil {
			return NewError(InTikvFatalError, "PutFreezer NewTrans err", err)
		}
		defer func() {
			if err == nil {
				err = c.CommitTrans(tx)
				if err != nil {
					err = NewError(InTikvFatalError, "PutFreezer err", err)
				}
			}
			if err != nil {
				c.AbortTrans(tx)
			}
		}()
	}
	txn := tx.(*TikvTx).tx
	val, err := txn.Get(context.TODO(), key)
	if err != nil && !kv.IsErrNotFound(err) {
		return NewError(InTikvFatalError, "PutFreezer Get err", err)
	}
	if kv.IsErrNotFound(err) {
		return nil
	}
	var f Freezer
	err = helper.MsgPackUnMarshal(val, &f)
	if err != nil {
		return NewError(InTikvFatalError, "PutFreezer MsgPackUnMarshal err", err)
	}
	f.Status = status
	f.LastModifiedTime = freezer.LastModifiedTime
	f.Location = freezer.Location
	f.Pool = freezer.Pool
	f.Size = freezer.Size
	f.ObjectId = freezer.ObjectId
	f.Parts = freezer.Parts
	newVal, err := helper.MsgPackMarshal(f)
	if err != nil {
		return NewError(InTikvFatalError, "PutFreezer MsgPackUnMarshal err", err)
	}
	err = txn.Set(key, newVal)
	if err != nil {
		return NewError(InTikvFatalError, "PutFreezer Set err", err)
	}
	return nil
}

func (c *TiKVClient) UpdateFreezerStatus(bucketName, objectName, version string, status, statusSetting common.RestoreStatus) (err error) {
	key := GenFreezerKey(bucketName, objectName, version)
	tx, err := c.NewTrans()
	if err != nil {
		return NewError(InTikvFatalError, "UpdateFreezerStatus NewTrans err", err)
	}
	defer func() {
		if err == nil {
			err = c.CommitTrans(tx)
			if err != nil {
				err = NewError(InTikvFatalError, "UpdateFreezerStatus err", err)
			}
		}
		if err != nil {
			c.AbortTrans(tx)
		}
	}()

	txn := tx.(*TikvTx).tx
	val, err := txn.Get(context.TODO(), key)
	if err != nil && !kv.IsErrNotFound(err) {
		return NewError(InTikvFatalError, "UpdateFreezerStatus Get err", err)
	}
	if kv.IsErrNotFound(err) {
		return nil
	}
	var f Freezer
	err = helper.MsgPackUnMarshal(val, &f)
	if err != nil {
		return NewError(InTikvFatalError, "UpdateFreezerStatus MsgPackUnMarshal err", err)
	}

	if f.Status != status {
		return nil
	}

	f.Status = statusSetting
	newVal, err := helper.MsgPackMarshal(f)
	if err != nil {
		return NewError(InTikvFatalError, "UpdateFreezerStatus MsgPackMarshal err", err)
	}

	err = txn.Set(key, newVal)
	if err != nil {
		return NewError(InTikvFatalError, "UpdateFreezerStatus Set err", err)
	}
	return nil
}
