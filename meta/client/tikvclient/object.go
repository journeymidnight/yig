package tikvclient

import (
	"context"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

const NullVersion = "null"

func genObjectKey(bucketName, objectName, version string) []byte {
	if version == NullVersion {
		return GenKey(false, bucketName, objectName)
	} else {
		return GenKey(false, bucketName, objectName, version)
	}
}

//object
func (c *TiKVClient) GetObject(bucketName, objectName, version string) (*Object, error) {
	key := genObjectKey(bucketName, objectName, version)
	v, err := c.Get(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrNoSuchKey
	}
	var o Object
	err = helper.MsgPackUnMarshal(v, &o)
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func (c *TiKVClient) PutObject(object *Object, multipart *Multipart, updateUsage bool) error {
	objectKey := genObjectKey(object.BucketName, object.Name, object.VersionId)
	tx, err := c.txnCli.Begin(context.TODO())
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

	objectVal, err := helper.MsgPackMarshal(object)
	if err != nil {
		return err
	}

	err = tx.Set(objectKey, objectVal)
	if err != nil {
		return err
	}

	if multipart != nil {
		multipartKey := genMultipartKey(object.BucketName, object.Name, multipart.UploadId)
		tx.Delete(multipartKey)
	}

	if updateUsage {
		// TODO: TBD
	}

	return nil

}

func (c *TiKVClient) PutObjectWithoutMultiPart(object *Object) error {
	objectKey := genObjectKey(object.BucketName, object.Name, object.VersionId)
	return c.Put(objectKey, object)
}

func (c *TiKVClient) UpdateObject(object *Object, multipart *Multipart, updateUsage bool) (err error) {
	return c.PutObject(object, multipart, updateUsage)
}

func (c *TiKVClient) UpdateObjectWithoutMultiPart(object *Object) error {
	return c.PutObjectWithoutMultiPart(object)
}

func (c *TiKVClient) UpdateAppendObject(object *Object) error {
	return c.PutObjectWithoutMultiPart(object)
}

func (c *TiKVClient) RenameObject(object *Object, sourceObject string) (err error) {
	oldKey := genObjectKey(object.BucketName, sourceObject, NullVersion)
	newKey := genObjectKey(object.BucketName, object.Name, NullVersion)

	tx, err := c.txnCli.Begin(context.TODO())
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

	v, err := helper.MsgPackMarshal(object)
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

func (c *TiKVClient) DeleteObject(object *Object, tx DB) error {
	return nil
}

func (c *TiKVClient) UpdateObjectAcl(object *Object) error {
	return c.PutObjectWithoutMultiPart(object)
}

func (c *TiKVClient) UpdateObjectAttrs(object *Object) error {
	return c.PutObjectWithoutMultiPart(object)
}
