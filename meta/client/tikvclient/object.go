package tikvclient

import (
	"context"
	. "database/sql/driver"
	"sync"

	"github.com/journeymidnight/client-go/txnkv"

	"github.com/journeymidnight/client-go/key"
	"github.com/journeymidnight/yig/backend"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

// **Key**: {BucketName}\{ObjectName}
// **Versioned Key**: {BucketName}\{ObjectName}\{Version}
// Version = math.MaxUint64-o.CreateTime
func GenObjectKey(bucketName, objectName, version string) []byte {
	if version == NullVersion {
		return GenKey(bucketName, objectName)
	} else {
		return GenKey(bucketName, objectName, version)
	}
}

func GenHotObjectKey(bucketName, objectName, version string) []byte {
	if version == NullVersion {
		return GenKey(TableHotObjectPrefix, bucketName, objectName)
	} else {
		return GenKey(TableHotObjectPrefix, bucketName, objectName, version)
	}
}

//object
func (c *TiKVClient) GetObject(bucketName, objectName, version string, tx Tx) (*Object, error) {
	key := GenObjectKey(bucketName, objectName, version)
	var o Object
	var txn *txnkv.Transaction
	if tx != nil {
		txn = tx.(*TikvTx).tx
	}
	ok, err := c.TxGet(key, &o, txn)
	if err != nil {
		err = NewError(InTikvFatalError, "GetObject TxGet err", err)
		return nil, err
	}
	if !ok {
		return nil, ErrNoSuchKey
	}

	if o.Parts != nil && len(o.Parts) != 0 {
		var sortedPartNum = make([]int64, len(o.Parts))
		for k, v := range o.Parts {
			sortedPartNum[k-1] = v.Offset
		}
		o.PartsIndex = &SimpleIndex{Index: sortedPartNum}
	}
	return &o, nil
}

func (c *TiKVClient) GetLatestObjectVersion(bucketName, objectName string) (object *Object, err error) {
	tx, err := c.NewTrans()
	if err != nil {
		err = NewError(InTikvFatalError, "GetLatestObjectVersion NewTrans err", err)
		return nil, err
	}
	defer func() {
		if err == nil {
			err = c.CommitTrans(tx)
			if err != nil {
				err = NewError(InTikvFatalError, "GetLatestObjectVersion err", err)
			}
		}
		if err != nil {
			c.AbortTrans(tx)
		}
	}()
	txn := tx.(*TikvTx).tx

	var o, vo, retObj Object
	var nullObjExist bool
	var kvs []KV
	var e1, e2 error
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		objKey := GenObjectKey(bucketName, objectName, NullVersion)
		nullObjExist, e1 = c.TxGet(objKey, &o, txn)
		if e1 != nil {
			e1 = NewError(InTikvFatalError, "GetLatestObjectVersion err", err)
		}
	}()

	go func() {
		defer wg.Done()
		versionStartKey := GenObjectKey(bucketName, objectName, TableMinKeySuffix)
		versionEndKey := GenObjectKey(bucketName, objectName, TableMaxKeySuffix)
		kvs, e2 = c.TxScan(key.Key(versionStartKey), key.Key(versionEndKey), 1, txn)
		if e2 != nil {
			e2 = NewError(InTikvFatalError, "GetLatestObjectVersion err", err)
			return
		}
		if len(kvs) != 0 {
			e2 = helper.MsgPackUnMarshal(kvs[0].V, &vo)
		}
	}()
	wg.Wait()

	if e1 != nil {
		return nil, e1
	} else if e2 != nil {
		return nil, e2
	}

	if !nullObjExist && len(kvs) == 0 {
		return nil, ErrNoSuchKey
	} else if !nullObjExist {
		retObj = vo
	} else if len(kvs) == 0 {
		retObj = o
	} else {
		ro := helper.Ternary(o.CreateTime > vo.CreateTime, o, vo)
		retObj = ro.(Object)
	}

	if retObj.Parts != nil && len(retObj.Parts) != 0 {
		var sortedPartNum = make([]int64, len(retObj.Parts))
		for k, v := range retObj.Parts {
			sortedPartNum[k-1] = v.Offset
		}
		retObj.PartsIndex = &SimpleIndex{Index: sortedPartNum}
	}
	return &retObj, nil
}

func (c *TiKVClient) PutObject(object *Object, multipart *Multipart, updateUsage bool) (err error) {
	objectKey := GenObjectKey(object.BucketName, object.Name, object.VersionId)

	tx, err := c.NewTrans()
	if err != nil {
		err = NewError(InTikvFatalError, "PutObject NewTrans err", err)
		return err
	}
	defer func() {
		if err == nil {
			err = c.CommitTrans(tx)
			if err != nil {
				err = NewError(InTikvFatalError, "PutObject err", err)
			}
		}
		if err != nil {
			c.AbortTrans(tx)
		}
	}()

	txn := tx.(*TikvTx).tx
	if multipart != nil {
		object.Parts = multipart.Parts
		err := c.DeleteMultipart(multipart, tx)
		if err != nil {
			return err
		}
	}

	objectVal, err := helper.MsgPackMarshal(object)
	if err != nil {
		err = NewError(InTikvFatalError, "PutObject MsgPackMarshal err", err)
		return err
	}

	err = txn.Set(objectKey, objectVal)
	if err != nil {
		err = NewError(InTikvFatalError, "PutObject Set err", err)
		return err
	}

	if updateUsage {
		return c.UpdateUsage(object.BucketName, object.Size, tx)
	}
	return nil
}

func (c *TiKVClient) UpdateObject(object *Object, multipart *Multipart, updateUsage bool, tx Tx) (err error) {
	objectKey := GenObjectKey(object.BucketName, object.Name, object.VersionId)
	if tx == nil {
		tx, err = c.NewTrans()
		if err != nil {
			err = NewError(InTikvFatalError, "UpdateObject NewTrans err", err)
			return err
		}
		defer func() {
			if err == nil {
				err = c.CommitTrans(tx)
				if err != nil {
					err = NewError(InTikvFatalError, "UpdateObject err", err)
				}
			}
			if err != nil {
				c.AbortTrans(tx)
			}
		}()
	}

	txn := tx.(*TikvTx).tx
	if multipart != nil {
		object.Parts = multipart.Parts
		err := c.DeleteMultipart(multipart, tx)
		if err != nil {
			return err
		}
	}

	objectVal, err := helper.MsgPackMarshal(object)
	if err != nil {
		err = NewError(InTikvFatalError, "UpdateObject MsgPackMarshal err", err)
		return err
	}

	err = txn.Set(objectKey, objectVal)
	if err != nil {
		err = NewError(InTikvFatalError, "UpdateObject Set err", err)
		return err
	}

	if updateUsage {
		return c.UpdateUsage(object.BucketName, object.DeltaSize, tx)
	}
	return nil
}

func (c *TiKVClient) RenameObject(object *Object, sourceObject string) (err error) {
	oldKey := GenObjectKey(object.BucketName, sourceObject, NullVersion)
	newKey := GenObjectKey(object.BucketName, object.Name, NullVersion)

	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		err = NewError(InTikvFatalError, "RenameObject Begin err", err)
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit(context.Background())
		}
		if err != nil {
			err = NewError(InTikvFatalError, "RenameObject err", err)
			tx.Rollback()
		}
	}()

	v, err := helper.MsgPackMarshal(*object)
	if err != nil {
		err = NewError(InTikvFatalError, "RenameObject MsgPackMarshal err", err)
		return err
	}
	err = tx.Set(newKey, v)
	if err != nil {
		err = NewError(InTikvFatalError, "RenameObject Set err", err)
		return err
	}
	err = tx.Delete(oldKey)
	if err != nil {
		err = NewError(InTikvFatalError, "RenameObject Delete err", err)
		return err
	}
	return nil
}

func (c *TiKVClient) DeleteObject(object *Object, tx Tx) (err error) {
	key := GenObjectKey(object.BucketName, object.Name, object.VersionId)
	if tx == nil {
		tx, err = c.NewTrans()
		if err != nil {
			err = NewError(InTikvFatalError, "DeleteObject NewTrans err", err)
			return err
		}
		defer func() {
			if err == nil {
				err = c.CommitTrans(tx)
				if err != nil {
					err = NewError(InTikvFatalError, "DeleteObject err", err)
				}
			}
			if err != nil {
				c.AbortTrans(tx)
			}
		}()
	}

	txn := tx.(*TikvTx).tx
	err = txn.Delete(key)
	if err != nil {
		return NewError(InTikvFatalError, "DeleteObject Delete err", err)
	}
	return nil
}

func (c *TiKVClient) UpdateObjectAcl(object *Object) error {
	return c.PutObject(object, nil, true)
}

func (c *TiKVClient) UpdateObjectAttrs(object *Object) error {
	return c.PutObject(object, nil, true)
}

func (c *TiKVClient) DeleteObjectPart(object *Object, tx Tx) (err error) {
	// This function will do nothing in TiKV because object parts will be replaced when update object.
	object.Parts = nil
	return nil
}

func (c *TiKVClient) ReplaceObjectMetas(object *Object, tx Tx) (err error) {
	return c.PutObject(object, nil, false)
}

func (c *TiKVClient) UpdateFreezerObject(object *Object, tx Tx) (err error) {
	return c.PutObject(object, nil, true)
}

func (c *TiKVClient) AppendObject(object *Object, updateUsage bool) (err error) {
	objectKey := GenObjectKey(object.BucketName, object.Name, object.VersionId)
	hotKey := GenHotObjectKey(object.BucketName, object.Name, object.VersionId)
	tx, err := c.NewTrans()
	if err != nil {
		err = NewError(InTikvFatalError, "AppendObject NewTrans err", err)
		return err
	}
	defer func() {
		if err == nil {
			err = c.CommitTrans(tx)
			if err != nil {
				err = NewError(InTikvFatalError, "AppendObject err", err)
			}
		}
		if err != nil {
			c.AbortTrans(tx)
		}
	}()

	txn := tx.(*TikvTx).tx

	v, err := helper.MsgPackMarshal(*object)
	if err != nil {
		err = NewError(InTikvFatalError, "AppendObject MsgPackMarshal err", err)
		return err
	}
	err = txn.Set(objectKey, v)
	if err != nil {
		err = NewError(InTikvFatalError, "AppendObject Set err", err)
		return err
	}

	if object.Pool == backend.SMALL_FILE_POOLNAME {
		err = txn.Set(hotKey, v)
		if err != nil {
			err = NewError(InTikvFatalError, "AppendObject Set err", err)
			return err
		}
	}
	if updateUsage {
		err = c.UpdateUsage(object.BucketName, object.DeltaSize, tx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *TiKVClient) MigrateObject(object *Object) (err error) {
	objectKey := GenObjectKey(object.BucketName, object.Name, object.VersionId)
	hotKey := GenHotObjectKey(object.BucketName, object.Name, object.VersionId)
	tx, err := c.NewTrans()
	if err != nil {
		err = NewError(InTikvFatalError, "MigrateObject NewTrans err", err)
		return err
	}
	defer func() {
		if err == nil {
			err = c.CommitTrans(tx)
			if err != nil {
				err = NewError(InTikvFatalError, "MigrateObject err", err)
			}
		}
		if err != nil {
			c.AbortTrans(tx)
		}
	}()

	txn := tx.(*TikvTx).tx
	v, err := helper.MsgPackMarshal(*object)
	if err != nil {
		err = NewError(InTikvFatalError, "MigrateObject MsgPackMarshal err", err)
		return err
	}
	err = txn.Set(objectKey, v)
	if err != nil {
		err = NewError(InTikvFatalError, "MigrateObject Set err", err)
		return err
	}
	err = txn.Delete(hotKey)
	if err != nil {
		err = NewError(InTikvFatalError, "MigrateObject Delete err", err)
		return err
	}
	return nil
}

func (c *TiKVClient) RemoveHotObject(object *Object, tx Tx) (err error) {
	hotKey := GenHotObjectKey(object.BucketName, object.Name, object.VersionId)
	if tx == nil {
		err = c.TxDelete(hotKey)
		if err != nil {
			err = NewError(InTikvFatalError, "RemoveHotObject TxDelete err", err)
			return err
		}
		return nil
	} else {
		txn := tx.(*TikvTx).tx
		err = txn.Delete(hotKey)
		if err != nil {
			err = NewError(InTikvFatalError, "RemoveHotObject Delete err", err)
			return err
		}
		return nil
	}
}

func (c *TiKVClient) UpdateAppendObject(object *Object) (err error) {
	objectKey := GenObjectKey(object.BucketName, object.Name, object.VersionId)
	hotKey := GenHotObjectKey(object.BucketName, object.Name, object.VersionId)
	tx, err := c.NewTrans()
	if err != nil {
		err = NewError(InTikvFatalError, "UpdateAppendObject NewTrans err", err)
		return err
	}
	defer func() {
		if err == nil {
			err = c.CommitTrans(tx)
			if err != nil {
				err = NewError(InTikvFatalError, "UpdateAppendObject err", err)
			}
		}
		if err != nil {
			c.AbortTrans(tx)
		}
	}()

	txn := tx.(*TikvTx).tx
	v, err := helper.MsgPackMarshal(*object)
	if err != nil {
		err = NewError(InTikvFatalError, "UpdateAppendObject MsgPackMarshal err", err)
		return err
	}
	err = txn.Set(objectKey, v)
	if err != nil {
		err = NewError(InTikvFatalError, "UpdateAppendObject Set err", err)
		return err
	}
	if object.Pool == backend.SMALL_FILE_POOLNAME {
		err = txn.Set(hotKey, v)
		if err != nil {
			err = NewError(InTikvFatalError, "UpdateAppendObject Set err", err)
			return err
		}
	}
	return c.UpdateUsage(object.BucketName, object.DeltaSize, tx)
}
