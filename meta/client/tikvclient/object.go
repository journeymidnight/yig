package tikvclient

import (
	"context"
	. "database/sql/driver"
	"sync"

	"github.com/journeymidnight/yig/backend"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/tikv/client-go/key"
)

// **Key**: {BucketName}\{ObjectName}
// **Versioned Key**: {BucketName}\{ObjectName}\{Version}
// Version = math.MaxUint64-o.CreateTime
func genObjectKey(bucketName, objectName, version string) []byte {
	if version == NullVersion {
		return GenKey(bucketName, objectName)
	} else {
		return GenKey(bucketName, objectName, version)
	}
}

func genHotObjectKey(bucketName, objectName, version string) []byte {
	if version == NullVersion {
		return GenKey(TableHotObjectPrefix, bucketName, objectName)
	} else {
		return GenKey(TableHotObjectPrefix, bucketName, objectName, version)
	}
}

//object
func (c *TiKVClient) GetObject(bucketName, objectName, version string, tx Tx) (*Object, error) {
	key := genObjectKey(bucketName, objectName, version)
	var o Object
	txn := tx.(*TikvTx).tx
	ok, err := c.TxGet(key, &o, txn)
	if err != nil {
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
		return nil, err
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

	var o, vo, retObj Object
	var nullObjExist bool
	var kvs []KV
	var e1, e2 error
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		objKey := genObjectKey(bucketName, objectName, NullVersion)
		nullObjExist, e1 = c.TxGet(objKey, &o, txn)
	}()

	go func() {
		defer wg.Done()
		versionStartKey := genObjectKey(bucketName, objectName, TableMinKeySuffix)
		versionEndKey := genObjectKey(bucketName, objectName, TableMaxKeySuffix)
		kvs, e2 = c.TxScan(key.Key(versionStartKey), key.Key(versionEndKey), 1, txn)
		if e2 != nil {
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
	objectKey := genObjectKey(object.BucketName, object.Name, object.VersionId)

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
	if multipart != nil {
		object.Parts = multipart.Parts
		err := c.DeleteMultipart(multipart, tx)
		if err != nil {
			return err
		}
	}

	objectVal, err := helper.MsgPackMarshal(object)
	if err != nil {
		return err
	}

	err = txn.Set(objectKey, objectVal)
	if err != nil {
		return err
	}

	if updateUsage {
		return c.UpdateUsage(object.BucketName, object.Size, tx)
	}
	return nil
}

func (c *TiKVClient) UpdateObject(object *Object, multipart *Multipart, updateUsage bool, tx Tx) (err error) {
	objectKey := genObjectKey(object.BucketName, object.Name, object.VersionId)
	if tx == nil {
		tx, err = c.NewTrans()
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
		return err
	}

	err = txn.Set(objectKey, objectVal)
	if err != nil {
		return err
	}

	if updateUsage {
		return c.UpdateUsage(object.BucketName, object.DeltaSize, tx)
	}
	return nil
}

func (c *TiKVClient) RenameObject(object *Object, sourceObject string) (err error) {
	oldKey := genObjectKey(object.BucketName, sourceObject, NullVersion)
	newKey := genObjectKey(object.BucketName, object.Name, NullVersion)

	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit(context.Background())
		}
		if err != nil {
			tx.Rollback()
		}
	}()

	v, err := helper.MsgPackMarshal(*object)
	if err != nil {
		return err
	}
	err = tx.Set(newKey, v)
	if err != nil {
		return err
	}
	err = tx.Delete(oldKey)
	if err != nil {
		return err
	}
	return nil
}

func (c *TiKVClient) DeleteObject(object *Object, tx Tx) error {
	key := genObjectKey(object.BucketName, object.Name, object.VersionId)
	if tx == nil {
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
	}

	txn := tx.(*TikvTx).tx
	return txn.Delete(key)
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
	objectKey := genObjectKey(object.BucketName, object.Name, object.VersionId)
	hotKey := genHotObjectKey(object.BucketName, object.Name, object.VersionId)
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

	v, err := helper.MsgPackMarshal(*object)
	if err != nil {
		return err
	}
	err = txn.Set(objectKey, v)
	if err != nil {
		return err
	}

	if object.Pool == backend.SMALL_FILE_POOLNAME {
		err = txn.Set(hotKey, v)
		if err != nil {
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
	objectKey := genObjectKey(object.BucketName, object.Name, object.VersionId)
	hotKey := genHotObjectKey(object.BucketName, object.Name, object.VersionId)
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
	v, err := helper.MsgPackMarshal(*object)
	if err != nil {
		return err
	}
	err = txn.Set(objectKey, v)
	if err != nil {
		return err
	}
	err = txn.Delete(hotKey)
	if err != nil {
		return err
	}
	return nil
}

func (c *TiKVClient) RemoveHotObject(object *Object, tx Tx) (err error) {
	hotKey := genHotObjectKey(object.BucketName, object.Name, object.VersionId)
	if tx == nil {
		return c.TxDelete(hotKey)
	} else {
		txn := tx.(*TikvTx).tx
		return txn.Delete(hotKey)
	}
}

func (c *TiKVClient) UpdateAppendObject(object *Object) error {
	objectKey := genObjectKey(object.BucketName, object.Name, object.VersionId)
	hotKey := genHotObjectKey(object.BucketName, object.Name, object.VersionId)
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
	v, err := helper.MsgPackMarshal(*object)
	if err != nil {
		return err
	}
	err = txn.Set(objectKey, v)
	if err != nil {
		return err
	}
	if object.Pool == backend.SMALL_FILE_POOLNAME {
		err = txn.Set(hotKey, v)
		if err != nil {
			return err
		}
	}
	return c.UpdateUsage(object.BucketName, object.DeltaSize, tx)
}
