package tikvclient

import (
	"context"
	. "database/sql/driver"
	"encoding/hex"

	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/tikv/client-go/key"
)

// **Key**: m\\{BucketName}\\{ObjectName}\\{UploadTime}
// encodedTime = BigEndian(MaxUint64 - multipart.InitialTime)
func genMultipartKey(bucketName, objectName string, initialTime uint64) []byte {
	encodedTime := EncodeTime(initialTime)

	return GenKey(TableMultipartPrefix, bucketName, objectName, hex.EncodeToString(encodedTime))
}

// Key: p\{BucketName}\{ObjectName}\{UploadId}\{PartNumber}
func genObjectPartKey(bucketName, objectName, uploadId string, partNumber int) []byte {
	return GenKey(TableObjectPartPrefix, bucketName, objectName, uploadId, hex.EncodeToString(EncodeUint64(uint64(partNumber))))
}

const MaxPartLimit = 1000

//multipart
func (c *TiKVClient) GetMultipart(bucketName, objectName, uploadId string) (Multipart, error) {
	var multipart Multipart
	initialTime, err := GetInitialTimeFromUploadId(uploadId)
	if err != nil {
		return multipart, err
	}
	multipartKey := genMultipartKey(bucketName, objectName, initialTime)
	ok, err := c.TxGet(multipartKey, &multipart)
	if err != nil {
		return multipart, err
	}
	if !ok {
		return multipart, ErrNoSuchUpload
	}

	objectPartStartKey := genObjectPartKey(bucketName, objectName, uploadId, 0)
	objectPartEndKey := genObjectPartKey(bucketName, objectName, uploadId, MaxPartLimit)
	kvs, err := c.TxScan(objectPartStartKey, objectPartEndKey, MaxPartLimit)
	if err != nil {
		return multipart, err
	}
	if len(kvs) == 0 {
		return multipart, nil
	}

	var parts = make(map[int]*Part)
	for _, kv := range kvs {
		var part Part
		err = helper.MsgPackUnMarshal(kv.V, &part)
		if err != nil {
			return multipart, err
		}
		parts[part.PartNumber] = &part
	}
	multipart.Parts = parts
	return multipart, nil
}

func (c *TiKVClient) CreateMultipart(multipart Multipart) (err error) {
	key := genMultipartKey(multipart.BucketName, multipart.ObjectName, multipart.InitialTime)
	return c.TxPut(key, multipart)
}

func (c *TiKVClient) PutObjectPart(multipart *Multipart, part *Part) (err error) {
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

	partKey := genObjectPartKey(multipart.BucketName, multipart.ObjectName, multipart.UploadId, part.PartNumber)
	txn := tx.(*TikvTx).tx
	partVal, err := helper.MsgPackMarshal(part)
	if err != nil {
		return err
	}

	err = txn.Set(partKey, partVal)
	if err != nil {
		return err
	}

	return c.UpdateUsage(multipart.BucketName, part.Size, tx)
}

func (c *TiKVClient) DeleteMultipart(multipart *Multipart, tx Tx) error {
	multipartKey := genMultipartKey(multipart.BucketName, multipart.ObjectName, multipart.InitialTime)
	err := multipart.GenUploadId()
	if err != nil {
		return err
	}

	keyPrefix := genObjectPartKey(multipart.BucketName, multipart.ObjectName, multipart.UploadId, 0)
	endKey := genObjectPartKey(multipart.BucketName, multipart.ObjectName, multipart.UploadId, MaxPartLimit)
	if tx == nil {
		tx, err := c.NewTrans()
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
	it, err := txn.Iter(context.TODO(), key.Key(keyPrefix), key.Key(endKey))
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Valid() {
		err := txn.Delete(it.Key()[:])
		if err != nil {
			txn.Rollback()
			return err
		}
		it.Next(context.TODO())
	}
	return txn.Delete(multipartKey)
}

func (c *TiKVClient) ListMultipartUploads(bucketName, keyMarker, uploadIdMarker, prefix, delimiter, encodingType string, maxUploads int) (uploads []datatype.Upload, prefixes []string, isTruncated bool, nextKeyMarker, nextUploadIdMarker string, err error) {

	return
}
