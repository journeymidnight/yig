package tikvclient

import (
	"context"
	. "database/sql/driver"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/journeymidnight/client-go/key"
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

// **Key**: m\{BucketName}\{ObjectName}\{EncodedTime}
// EncodedTime = fmt.Sprintf("%020d", math.MaxUint64-initialTime)
// UploadId = hex.EncodeToString(xxtea.Encrypt([]byte(multipart.InitialTime), XXTEA_KEY))
func GenMultipartKey(bucketName, objectName string, initialTime uint64) []byte {
	return GenKey(TableMultipartPrefix, bucketName, objectName, fmt.Sprintf("%020d", math.MaxUint64-initialTime))
}

// **Key**: p\{BucketName}\{ObjectName}\{UploadId}\{EncodePartNumber}
// EncodePartNumber = fmt.Sprintf("%05d", partNumber)
func GenObjectPartKey(bucketName, objectName, uploadId string, partNumber int) []byte {
	return GenKey(TableObjectPartPrefix, bucketName, objectName, uploadId, fmt.Sprintf("%05d", partNumber))
}

const MaxPartLimit = 10000

//multipart
func (c *TiKVClient) GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error) {
	initialTime, err := GetInitialTimeFromUploadId(uploadId)
	if err != nil {
		return multipart, err
	}

	tx, err := c.NewTrans()
	if err != nil {
		err = NewError(InTikvFatalError, "GetMultipart NewTrans err", err)
		return multipart, err
	}
	defer func() {
		if err == nil {
			err = c.CommitTrans(tx)
			if err != nil {
				err = NewError(InTikvFatalError, "GetMultipart err", err)
			}
		}
		if err != nil {
			c.AbortTrans(tx)
		}
	}()
	txn := tx.(*TikvTx).tx

	multipartKey := GenMultipartKey(bucketName, objectName, initialTime)
	ok, err := c.TxGet(multipartKey, &multipart, txn)
	if err != nil {
		err = NewError(InTikvFatalError, "GetMultipart TxGet err", err)
		return multipart, err
	}
	if !ok {
		return multipart, ErrNoSuchUpload
	}

	objectPartStartKey := GenObjectPartKey(bucketName, objectName, uploadId, 0)
	objectPartEndKey := GenObjectPartKey(bucketName, objectName, uploadId, MaxPartLimit)
	kvs, err := c.TxScan(objectPartStartKey, objectPartEndKey, MaxPartLimit, txn)
	if err != nil {
		err = NewError(InTikvFatalError, "GetMultipart TxScan err", err)
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
			err = NewError(InTikvFatalError, "GetMultipart MsgPackUnMarshal err", err)
			return multipart, err
		}
		parts[part.PartNumber] = &part
	}
	multipart.Parts = parts
	return multipart, nil
}

func (c *TiKVClient) CreateMultipart(multipart Multipart) (err error) {
	key := GenMultipartKey(multipart.BucketName, multipart.ObjectName, multipart.InitialTime)
	err = c.TxPut(key, multipart)
	if err != nil {
		return NewError(InTikvFatalError, "CreateMultipart TxPut err", err)
	}
	return nil
}

func (c *TiKVClient) PutObjectPart(multipart *Multipart, part *Part) (deltaSize int64, err error) {
	tx, err := c.NewTrans()
	if err != nil {
		err = NewError(InTikvFatalError, "PutObjectPart NewTrans err", err)
		return 0, err
	}
	defer func() {
		if err == nil {
			err = c.CommitTrans(tx)
			if err != nil {
				err = NewError(InTikvFatalError, "PutObjectPart err", err)
			}
		}
		if err != nil {
			c.AbortTrans(tx)
		}
	}()
	txn := tx.(*TikvTx).tx

	partKey := GenObjectPartKey(multipart.BucketName, multipart.ObjectName, multipart.UploadId, part.PartNumber)
	partVal, err := helper.MsgPackMarshal(part)
	if err != nil {
		err = NewError(InTikvFatalError, "PutObjectPart MsgPackMarshal err", err)
		return 0, err
	}

	err = txn.Set(partKey, partVal)
	if err != nil {
		err = NewError(InTikvFatalError, "PutObjectPart Set err", err)
		return 0, err
	}
	var removedSize int64 = 0
	if part, ok := multipart.Parts[part.PartNumber]; ok {
		removedSize += part.Size
	}
	deltaSize = part.Size - removedSize
	err = c.UpdateUsage(multipart.BucketName, deltaSize, tx)
	if err != nil {
		return
	}
	return deltaSize, nil
}

func (c *TiKVClient) DeleteMultipart(multipart *Multipart, tx Tx) (err error) {
	multipartKey := GenMultipartKey(multipart.BucketName, multipart.ObjectName, multipart.InitialTime)
	err = multipart.GenUploadId()
	if err != nil {
		return err
	}

	keyPrefix := GenObjectPartKey(multipart.BucketName, multipart.ObjectName, multipart.UploadId, 0)
	endKey := GenObjectPartKey(multipart.BucketName, multipart.ObjectName, multipart.UploadId, MaxPartLimit)
	if tx == nil {
		tx, err := c.NewTrans()
		if err != nil {
			return err
		}
		defer func() {
			if err == nil {
				err = c.CommitTrans(tx)
				if err != nil {
					err = NewError(InTikvFatalError, "DeleteMultipart err", err)
				}
			}
			if err != nil {
				c.AbortTrans(tx)
			}
		}()
	}
	txn := tx.(*TikvTx).tx
	it, err := txn.Iter(context.TODO(), key.Key(keyPrefix), key.Key(endKey))
	if err != nil {
		return NewError(InTikvFatalError, "DeleteMultipart Iter err", err)
	}
	defer it.Close()
	for it.Valid() {
		err := txn.Delete(it.Key())
		if err != nil {
			txn.Rollback()
			return NewError(InTikvFatalError, "DeleteMultipart Delete err", err)
		}
		if err := it.Next(context.TODO()); err != nil && it.Valid() {
			return NewError(InTikvFatalError, "DeleteMultipart get next err", err)
		}
	}
	err = txn.Delete(multipartKey)
	if err != nil {
		return NewError(InTikvFatalError, "DeleteMultipart Delete err", err)
	}
	return nil
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

	startKey := GenMultipartKey(bucketName, keyMarker, initialTime)
	endKey := GenMultipartKey(bucketName, TableMaxKeySuffix, math.MaxUint64)

	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		err = NewError(InTikvFatalError, "ListMultipartUploads TCBegin err", err)
		return result, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		err = NewError(InTikvFatalError, "ListMultipartUploads Iter err", err)
		return result, err
	}
	defer it.Close()

	var commonPrefixes []string

	count := 0
	lastKey := ""
	lastUploadId := ""
	// Key: m\\{BucketName}\\{ObjectName}\\{UploadTime}
	for it.Valid() {
		k, v := string(it.Key()), it.Value()
		if k == string(startKey) {
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				err = NewError(InTikvFatalError, "ListMultipartUploads get next err", err)
				return result, err
			}
			continue
		}
		sp := strings.Split(k, TableSeparator)
		if len(sp) != 4 {
			helper.Logger.Error("Invalid multipart key:", k)
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				err = NewError(InTikvFatalError, "ListMultipartUploads get next err", err)
				return result, err
			}
			continue
		}
		objectName := sp[2]
		if !strings.HasPrefix(objectName, prefix) {
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				err = NewError(InTikvFatalError, "ListMultipartUploads get next err", err)
				return result, err
			}
			continue
		}

		if delimiter != "" {
			subKey := strings.TrimPrefix(objectName, prefix)
			sp := strings.Split(subKey, delimiter)
			if len(sp) > 2 {
				if err := it.Next(context.TODO()); err != nil && it.Valid() {
					err = NewError(InTikvFatalError, "ListMultipartUploads get next err", err)
					return result, err
				}
				continue
			} else if len(sp) == 2 {
				if sp[1] == "" {
					lastKey = objectName
					commonPrefixes = append(commonPrefixes, subKey)
					count++
					if count == maxUploads {
						break
					}
					if err := it.Next(context.TODO()); err != nil && it.Valid() {
						err = NewError(InTikvFatalError, "ListMultipartUploads get next err", err)
						return result, err
					}
					continue
				} else {
					if err := it.Next(context.TODO()); err != nil && it.Valid() {
						err = NewError(InTikvFatalError, "ListMultipartUploads get next err", err)
						return result, err
					}
					continue
				}
			}
		}

		var m Multipart
		var u datatype.Upload
		err = helper.MsgPackUnMarshal(v, &m)
		if err != nil {
			err = NewError(InTikvFatalError, "ListMultipartUploads MsgPackUnMarshal err", err)
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
		if err := it.Next(context.TODO()); err != nil && it.Valid() {
			err = NewError(InTikvFatalError, "ListMultipartUploads get next err", err)
			return result, err
		}
		continue
	}
	sort.Strings(commonPrefixes)
	for _, prefix := range commonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, datatype.CommonPrefix{
			Prefix: prefix,
		})
	}
	if err := it.Next(context.TODO()); err != nil && it.Valid() {
		err = NewError(InTikvFatalError, "ListMultipartUploads get next err", err)
		return result, err
	}
	if it.Valid() {
		result.NextKeyMarker = lastKey
		result.IsTruncated = true
		result.NextUploadIdMarker = lastUploadId
	}
	return
}
