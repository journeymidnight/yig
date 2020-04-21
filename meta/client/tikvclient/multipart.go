package tikvclient

import (
	"context"
	. "database/sql/driver"
	"encoding/hex"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/tikv/client-go/key"
)

// **Key**: m\{BucketName}\{ObjectName}\{EncodedTime}
// UploadTime = MaxUint64 - multipart.InitialTime
// EncodedTime = hex.EncodeToString(BigEndian(UploadTime)）
func genMultipartKey(bucketName, objectName string, initialTime uint64) []byte {
	encodedTime := hex.EncodeToString(EncodeUint64(math.MaxUint64 - initialTime))
	return GenKey(TableMultipartPrefix, bucketName, objectName, encodedTime)
}

// **Key**: p\{BucketName}\{ObjectName}\{UploadId}\{EncodePartNumber}
// EncodePartNumber = hex.EncodeToString(BigEndian({PartNumber}))
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

func (c *TiKVClient) ListMultipartUploads(bucketName, keyMarker, uploadIdMarker, prefix, delimiter, encodingType string, maxUploads int) (result datatype.ListMultipartUploadsResponse, err error) {
	var initialTime uint64
	if uploadIdMarker != "" {
		initialTime, err = GetInitialTimeFromUploadId(uploadIdMarker)
		if err != nil {
			return result, err
		}
	}

	result.Prefix = prefix
	result.Bucket = bucketName
	result.KeyMarker = keyMarker
	result.MaxUploads = maxUploads
	result.Delimiter = delimiter

	startKey := genMultipartKey(bucketName, keyMarker, initialTime)
	endKey := genMultipartKey(bucketName, TableMaxKeySuffix, math.MaxUint64)

	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return result, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		return result, err
	}
	defer it.Close()

	var commonPrefixes []string

	count := 0
	lastKey := ""
	lastUploadId := ""
	// Key: m\\{BucketName}\\{ObjectName}\\{UploadTime}
	for it.Valid() {
		k, v := string(it.Key()[:]), it.Value()
		if k == string(startKey) {
			it.Next(context.TODO())
			continue
		}
		sp := strings.Split(k, TableSeparator)
		if len(sp) != 4 {
			helper.Logger.Error("Invalid multipart key:", k)
			it.Next(context.TODO())
			continue
		}
		objectName := sp[2]
		if !strings.HasPrefix(objectName, prefix) {
			it.Next(context.TODO())
			continue
		}

		if delimiter != "" {
			subKey := strings.TrimPrefix(objectName, prefix)
			sp := strings.Split(subKey, delimiter)
			if len(sp) > 2 {
				it.Next(context.TODO())
				continue
			} else if len(sp) == 2 {
				if sp[1] == "" {
					lastKey = objectName
					commonPrefixes = append(commonPrefixes, subKey)
					count++
					if count == maxUploads {
						break
					}
					it.Next(context.TODO())
					continue
				} else {
					it.Next(context.TODO())
					continue
				}
			}
		}

		var m Multipart
		var u datatype.Upload
		err = helper.MsgPackUnMarshal(v, &m)
		if err != nil {
			return result, err
		}
		lastKey = objectName
		lastUploadId = m.UploadId

		u.UploadId = m.UploadId
		u.Key = m.ObjectName
		u.StorageClass = m.Metadata.StorageClass.ToString()
		u.Owner = datatype.Owner{ID: m.Metadata.OwnerId}
		s := int64(m.InitialTime / 1e9)
		ns := int64(m.InitialTime % 1e9)
		u.Initiated = time.Unix(s, ns).UTC().Format(CREATE_TIME_LAYOUT)
		u.Initiator.ID = m.Metadata.InitiatorId
		result.Uploads = append(result.Uploads, u)
		count++
		if count == maxUploads {
			break
		}
		it.Next(context.TODO())
		continue
	}
	sort.Strings(commonPrefixes)
	for _, prefix := range commonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, datatype.CommonPrefix{
			Prefix: prefix,
		})
	}
	it.Next(context.TODO())
	if it.Valid() {
		result.NextKeyMarker = lastKey
		result.IsTruncated = true
		result.NextUploadIdMarker = lastUploadId
	}
	return
}
