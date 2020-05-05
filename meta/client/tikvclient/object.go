package tikvclient

import (
	"context"
	. "database/sql/driver"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/tikv/client-go/key"
)

// **Key**: {BucketName}\{ObjectName}
// **Versioned Key**: v\{BucketName}\{ObjectName}\{Version}
// Version = hex.EncodeToString(BigEndian(MaxUint64 - object.LastModifiedTime.UnixNano()))
func genObjectKey(bucketName, objectName, version string) []byte {
	if version == NullVersion {
		return GenKey(bucketName, objectName)
	} else {
		return GenKey(TableVersionObjectPrefix, bucketName, objectName, version)
	}
}

//object
func (c *TiKVClient) GetObject(bucketName, objectName, version string) (*Object, error) {
	key := genObjectKey(bucketName, objectName, version)
	var o Object
	ok, err := c.TxGet(key, &o)
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
	objKey := genObjectKey(bucketName, objectName, NullVersion)
	var o, vo Object
	nullObjExist, err := c.TxGet(objKey, &o)
	if err != nil {
		return nil, err
	}
	versionStartKey := genObjectKey(bucketName, objectName, TableMinKeySuffix)
	versionEndKey := genObjectKey(bucketName, objectName, TableMaxKeySuffix)
	kvs, err := c.TxScan(key.Key(versionStartKey), key.Key(versionEndKey), 1)
	if err != nil {
		return nil, err
	}
	if !nullObjExist && len(kvs) == 0 {
		return nil, ErrNoSuchKey
	} else if !nullObjExist {
		err = helper.MsgPackUnMarshal(kvs[0].V, &vo)
		if err != nil {
			return nil, err
		}
		return &vo, nil
	} else if len(kvs) == 0 {
		return &o, nil
	} else {
		retObj := helper.Ternary(o.LastModifiedTime.After(vo.LastModifiedTime), &o, &vo)
		return retObj.(*Object), nil
	}
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
	return c.PutObject(object, multipart, updateUsage)
}

func (c *TiKVClient) UpdateAppendObject(object *Object) error {
	return c.PutObject(object, nil, true)
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
	return nil
}

func (c *TiKVClient) MigrateObject(object *Object) (err error) {
	return nil
}
