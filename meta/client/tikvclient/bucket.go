package tikvclient

import (
	"context"
	. "database/sql/driver"
	"github.com/journeymidnight/client-go/key"
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/client"
	. "github.com/journeymidnight/yig/meta/types"
	"math"
	"strings"
)

// **Key**: b\{BucketName}
func GenBucketKey(bucketName string) []byte {
	return GenKey(TableBucketPrefix, bucketName)
}

//bucket
func (c *TiKVClient) GetBucket(bucketName string) (*Bucket, error) {
	bucketKey := GenBucketKey(bucketName)
	var b Bucket
	ok, err := c.TxGet(bucketKey, &b, nil)
	if err != nil {
		err = NewError(InTikvFatalError, "GetBucket TxGet err", err)
		return nil, err
	}
	if !ok {
		return nil, ErrNoSuchBucket
	}
	return &b, nil
}

// TODO: To be deprecated
func (c *TiKVClient) GetBuckets() (buckets []Bucket, err error) {
	startKey := GenKey(TableBucketPrefix, TableMinKeySuffix)
	endKey := GenKey(TableBucketPrefix, TableMaxKeySuffix)
	kvs, err := c.TxScan(startKey, endKey, math.MaxInt64, nil)
	for _, kv := range kvs {
		var b Bucket
		err = helper.MsgPackUnMarshal(kv.V, &b)
		if err != nil {
			err = NewError(InTikvFatalError, "GetBuckets MsgPackUnMarshal err", err)
			return nil, err
		}
		buckets = append(buckets, b)
	}
	return buckets, nil
}

func (c *TiKVClient) PutBucket(bucket Bucket) error {
	bucketKey := GenBucketKey(bucket.Name)
	err := c.TxPut(bucketKey, bucket)
	if err != nil {
		return NewError(InTikvFatalError, "PutBucket TxPut err", err)
	}
	return nil
}

// for commercial billing now
type BucketUsage struct {
	Standard   int64
	StandardIa int64
	Glacier    int64
}

func (c *TiKVClient) PutNewBucket(bucket Bucket) error {
	bucketKey := GenBucketKey(bucket.Name)
	userBucketKey := GenUserBucketKey(bucket.OwnerId, bucket.Name)
	existBucket, err := c.TxExist(bucketKey)
	if err != nil {
		err = NewError(InTikvFatalError, "PutNewBucket TxExist err", err)
		return err
	}
	existUserBucket, err := c.TxExist(userBucketKey)
	if err != nil {
		err = NewError(InTikvFatalError, "PutNewBucket TxExist err", err)
		return err
	}
	if existBucket && existUserBucket {
		return ErrBucketAlreadyExists
	}
	err = c.TxPut(bucketKey, bucket, userBucketKey, BucketUsage{0, 0, 0})
	if err != nil {
		return NewError(InTikvFatalError, "PutNewBucket TxPut err", err)
	}
	return nil
}

func (c *TiKVClient) DeleteBucket(bucket Bucket) error {
	bucketKey := GenBucketKey(bucket.Name)
	userBucketKey := GenUserBucketKey(bucket.OwnerId, bucket.Name)
	lifeCycleKey := GenLifecycleKey(bucket.Name)
	err := c.TxDelete(bucketKey, userBucketKey, lifeCycleKey)
	if err != nil {
		return NewError(InTikvFatalError, "DeleteBucket TxDelete err", err)
	}
	return nil
}

func (c *TiKVClient) ListHotObjects(marker string, maxKeys int) (listInfo ListHotObjectsInfo, err error) {
	var startKey []byte
	if marker == "" {
		startKey = GenHotObjectKey(TableMinKeySuffix, TableMinKeySuffix, TableMinKeySuffix)
	} else {
		startKey = []byte(marker)
	}

	endKey := GenHotObjectKey(TableMaxKeySuffix, TableMaxKeySuffix, TableMaxKeySuffix)
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		err := NewError(InTikvFatalError, "ListHotObjects TCBegin err", err)
		return listInfo, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		err := NewError(InTikvFatalError, "ListHotObjects Iter err", err)
		return listInfo, err
	}
	defer it.Close()

	count := 0
	for it.Valid() {
		k, v := string(it.Key()), it.Value()
		if k == string(startKey) {
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				return listInfo, err
			}
			continue
		}
		var o Object
		err = helper.MsgPackUnMarshal(v, &o)
		if err != nil {
			err := NewError(InTikvFatalError, "ListHotObjects MsgPackUnMarshal err", err)
			return listInfo, err
		}
		count++
		if count == maxKeys {
			listInfo.NextMarker = k
		}
		if count > maxKeys {
			listInfo.IsTruncated = true
			break
		}
		listInfo.Objects = append(listInfo.Objects, &o)
		if err := it.Next(context.TODO()); err != nil && it.Valid() {
			err := NewError(InTikvFatalError, "ListHotObjects get next err", err)
			return listInfo, err
		}
	}
	return
}

func AddEndByteValue(str string) string {
	if str == "" {
		return str
	}
	b := []byte(str)
	e := b[len(b)-1]
	if e != 255 {
		e++
		b = append(b[:len(b)-1], e)
		return string(b)
	}
	return str
}

func (c *TiKVClient) ListObjects(bucketName, marker, prefix, delimiter string, maxKeys int) (listInfo ListObjectsInfo, err error) {
	scanStart := marker + string(byte(0))
	if prefix != "" {
		if marker == "" || strings.Compare(marker, prefix) < 0 {
			scanStart = prefix
		} else if !strings.HasPrefix(marker, prefix) && strings.Compare(marker, prefix) > 0 {
			return listInfo, nil
		}
	}

	startKey := GenObjectKey(bucketName, scanStart, NullVersion)
	endKey := GenObjectKey(bucketName, prefix+TableMaxKeySuffix, NullVersion)
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		err := NewError(InTikvFatalError, "ListObjects TCBegin err", err)
		return listInfo, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		err := NewError(InTikvFatalError, "ListObjects Iter err", err)
		return listInfo, err
	}
	defer it.Close()
	commonPrefixes := make(map[string]interface{})

	count := 0
	for it.Valid() {
		k, v := string(it.Key()), it.Value()
		// extract object key
		objKey := strings.SplitN(k, TableSeparator, 2)[1]
		if delimiter != "" {
			subKey := strings.TrimPrefix(objKey, prefix)
			sp := strings.SplitN(subKey, delimiter, 2)
			if len(sp) == 2 {
				prefixKey := prefix + sp[0] + delimiter
				if prefixKey == marker {
					startKey = GenObjectKey(bucketName, AddEndByteValue(prefixKey), NullVersion)
					it.Close()
					it, err = tx.Iter(context.TODO(), startKey, endKey)
					if err != nil {
						err := NewError(InTikvFatalError, "ListObjects Iter err", err)
						return listInfo, err
					}
					continue
				}
				if _, ok := commonPrefixes[prefixKey]; !ok {
					count++
					if count == maxKeys {
						listInfo.NextMarker = prefixKey
					}
					if count > maxKeys {
						listInfo.IsTruncated = true
						break
					}
					commonPrefixes[prefixKey] = nil
				}
				startKey = GenObjectKey(bucketName, AddEndByteValue(prefixKey), NullVersion)
				it.Close()
				it, err = tx.Iter(context.TODO(), startKey, endKey)
				if err != nil {
					err := NewError(InTikvFatalError, "ListObjects Iter err", err)
					return listInfo, err
				}
				continue
			}
		}
		var o Object
		err = helper.MsgPackUnMarshal(v, &o)
		if err != nil {
			err := NewError(InTikvFatalError, "ListObjects MsgPackUnMarshal err", err)
			return listInfo, err
		}

		info_o := ModifyMetaToObjectResult(o)
		count++
		if count == maxKeys {
			listInfo.NextMarker = objKey
		}
		if count > maxKeys {
			listInfo.IsTruncated = true
			break
		}
		listInfo.Objects = append(listInfo.Objects, info_o)
		if err := it.Next(context.TODO()); err != nil && it.Valid() {
			err := NewError(InTikvFatalError, "ListObjects get next err", err)
			return listInfo, err
		}
	}
	listInfo.Prefixes = helper.Keys(commonPrefixes)
	return
}

func (c *TiKVClient) ListLatestObjects(bucketName, marker, prefix, delimiter string, maxKeys int) (listInfo ListObjectsInfo, err error) {
	startVersion := TableMaxKeySuffix
	if prefix != "" {
		if marker == "" || strings.Compare(marker, prefix) < 0 {
			marker = prefix
			// HACK: we want to scan ALL versions of a key, for GenObjectKey's current implement,
			//       set version to `NullVersion`
			startVersion = NullVersion
		} else if !strings.HasPrefix(marker, prefix) && strings.Compare(marker, prefix) > 0 {
			err := NewError(InTikvFatalError, "ListLatestObjects HasPrefix err", err)
			return listInfo, err
		}
	}

	startKey := GenObjectKey(bucketName, marker, startVersion)
	endKey := GenObjectKey(bucketName, prefix+TableMaxKeySuffix, TableMaxKeySuffix)

	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		err := NewError(InTikvFatalError, "ListLatestObjects TCBegin err", err)
		return listInfo, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		err := NewError(InTikvFatalError, "ListLatestObjects Iter err", err)
		return listInfo, err
	}
	defer it.Close()

	commonPrefixes := make(map[string]interface{})
	count := 0
	var previousNullObjectMeta *Object
	for it.Valid() {
		k, v := string(it.Key()), it.Value()
		var objMeta Object
		err = helper.MsgPackUnMarshal(v, &objMeta)
		if err != nil {
			err := NewError(InTikvFatalError, "ListLatestObjects MsgPackUnMarshal err", err)
			return listInfo, err
		}

		if previousNullObjectMeta != nil {
			var meta Object
			var passKey bool
			if objMeta.Name != previousNullObjectMeta.Name {
				meta = *previousNullObjectMeta
			} else {
				if objMeta.CreateTime > previousNullObjectMeta.CreateTime {
					meta = objMeta
				} else {
					meta = *previousNullObjectMeta
				}
				passKey = true
			}

			if !meta.DeleteMarker {
				o := ModifyMetaToObjectResult(meta)

				count++
				if count == maxKeys {
					listInfo.NextMarker = o.Key
				}

				if count > maxKeys {
					previousNullObjectMeta = nil
					listInfo.IsTruncated = true
					break
				}
				listInfo.Objects = append(listInfo.Objects, o)
			}

			previousNullObjectMeta = nil
			// Compare once
			if passKey {
				startKey = GenObjectKey(bucketName, objMeta.Name, TableMaxKeySuffix)
				it.Close()
				it, err = tx.Iter(context.TODO(), startKey, endKey)
				if err != nil {
					err := NewError(InTikvFatalError, "ListLatestObjects Iter err", err)
					return listInfo, err
				}
				continue
			}
		}

		// extract object key
		keySp := strings.Split(k, TableSeparator)
		objKey := keySp[1]

		if delimiter != "" {
			subKey := strings.TrimPrefix(objKey, prefix)
			sp := strings.SplitN(subKey, delimiter, 2)
			if len(sp) == 2 {
				prefixKey := prefix + sp[0] + delimiter
				if prefixKey == marker {
					startKey = GenObjectKey(bucketName, AddEndByteValue(prefixKey), NullVersion)
					it.Close()
					it, err = tx.Iter(context.TODO(), startKey, endKey)
					if err != nil {
						err := NewError(InTikvFatalError, "ListLatestObjects Iter err", err)
						return listInfo, err
					}
					continue
				}
				if _, ok := commonPrefixes[prefixKey]; !ok {
					count++
					if count == maxKeys {
						listInfo.NextMarker = prefixKey
					}
					if count > maxKeys {
						listInfo.IsTruncated = true
						break
					}
					commonPrefixes[prefixKey] = nil
				}
				startKey = GenObjectKey(bucketName, AddEndByteValue(prefixKey), NullVersion)
				it.Close()
				it, err = tx.Iter(context.TODO(), startKey, endKey)
				if err != nil {
					err := NewError(InTikvFatalError, "ListLatestObjects Iter err", err)
					return listInfo, err
				}
				continue
			}
		}

		// null version object
		if len(keySp) == 2 {
			previousNullObjectMeta = &objMeta
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				err := NewError(InTikvFatalError, "ListLatestObjects get next err", err)
				return listInfo, err
			}
			continue
		}

		// If not null version object
		startKey = GenObjectKey(bucketName, objMeta.Name, TableMaxKeySuffix)
		it.Close()
		it, err = tx.Iter(context.TODO(), startKey, endKey)
		if err != nil {
			err := NewError(InTikvFatalError, "ListLatestObjects Iter err", err)
			return listInfo, err
		}

		// If delete marker, do continue
		if objMeta.DeleteMarker {
			continue
		}

		var o = ModifyMetaToObjectResult(objMeta)

		count++
		if count == maxKeys {
			listInfo.NextMarker = objMeta.Name
		}

		if count > maxKeys {
			listInfo.IsTruncated = true
			break
		}

		listInfo.Objects = append(listInfo.Objects, o)
	}

	// If the final object is null version
	if previousNullObjectMeta != nil && !previousNullObjectMeta.DeleteMarker {
		o := ModifyMetaToObjectResult(*previousNullObjectMeta)
		count++
		if count == maxKeys {
			listInfo.NextMarker = o.Key
		}

		if count > maxKeys {
			listInfo.IsTruncated = true
			listInfo.Prefixes = helper.Keys(commonPrefixes)
			return listInfo, nil
		}
		listInfo.Objects = append(listInfo.Objects, o)
	}

	listInfo.Prefixes = helper.Keys(commonPrefixes)
	return
}

func (c *TiKVClient) ListVersionedObjects(bucketName, marker, verIdMarker, prefix, delimiter string,
	maxKeys int) (listInfo VersionedListObjectsInfo, err error) {
	if prefix != "" {
		if marker == "" || strings.Compare(marker, prefix) < 0 {
			marker = prefix
			verIdMarker = ""
		} else if !strings.HasPrefix(marker, prefix) && strings.Compare(marker, prefix) > 0 {
			err := NewError(InTikvFatalError, "ListVersionedObjects HasPrefix err", err)
			return listInfo, err
		}
	}

	commonPrefixes := make(map[string]interface{})
	var count int
	var exit bool
	var previousNullObjectMeta *Object
	var startKey, endKey []byte

	isPrefixMarker := delimiter != "" && strings.HasSuffix(marker, delimiter)
	if marker != "" && !isPrefixMarker {
		var needCompareNull = true
		var nullObjMeta *Object
		txn, err := c.NewTrans()
		if err != nil {
			err := NewError(InTikvFatalError, "ListVersionedObjects NewTrans err", err)
			return listInfo, err
		}
		nullObjMeta, err = c.GetObject(bucketName, marker, NullVersion, txn)
		if err != nil && err != ErrNoSuchKey {
			return listInfo, err
		}
		if err == ErrNoSuchKey {
			if verIdMarker == NullVersion {
				return listInfo, nil
			} else {
				needCompareNull = false
			}
		} else {
			// Calculate the null object version to compare with other versioned object
			nullVerIdMarker := nullObjMeta.GenVersionId(datatype.BucketVersioningEnabled)
			if verIdMarker == NullVersion {
				needCompareNull = false
				verIdMarker = nullVerIdMarker
			} else if nullVerIdMarker < verIdMarker {
				// currentVerIdMarker is older than null object
				needCompareNull = false
			}
		}

		// HACK: `verIdMarker` == "" means user wants to scan ALL versions of a key,
		//       for GenObjectKey's current implement, set version to `NullVersion`
		versionScanStart := verIdMarker
		if versionScanStart == "" {
			versionScanStart = NullVersion
		}
		startKey = GenObjectKey(bucketName, marker, versionScanStart)
		endKey = GenObjectKey(bucketName, marker, TableMaxKeySuffix)
		tx := txn.(*TikvTx).tx
		it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
		if err != nil {
			err := NewError(InTikvFatalError, "ListVersionedObjects Iter err", err)
			return listInfo, err
		}
		defer it.Close()

		for it.Valid() {
			v := it.Value()
			var VerObjMeta Object
			err = helper.MsgPackUnMarshal(v, &VerObjMeta)
			if err != nil {
				err := NewError(InTikvFatalError, "ListVersionedObjects MsgPackUnMarshal err", err)
				return listInfo, err
			}
			if VerObjMeta.Name == marker && VerObjMeta.VersionId == verIdMarker {
				if err := it.Next(context.TODO()); err != nil && it.Valid() {
					err := NewError(InTikvFatalError, "ListVersionedObjects get next err", err)
					return listInfo, err
				}
				continue
			}
			var o datatype.VersionedObject
			if needCompareNull && nullObjMeta.CreateTime > VerObjMeta.CreateTime {
				needCompareNull = false
				o = ModifyMetaToVersionedObjectResult(*nullObjMeta)
			} else {
				o = ModifyMetaToVersionedObjectResult(VerObjMeta)
			}
			count++
			if count == maxKeys {
				listInfo.NextKeyMarker = o.Key
				listInfo.NextVersionIdMarker = o.VersionId
			}
			if count > maxKeys {
				listInfo.IsTruncated = true
				exit = true
				break
			}
			listInfo.Objects = append(listInfo.Objects, o)
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				err := NewError(InTikvFatalError, "ListVersionedObjects get next err", err)
				return listInfo, err
			}
		}
	}

	if exit {
		return listInfo, nil
	}

	startKey = GenObjectKey(bucketName, marker, TableMaxKeySuffix)
	endKey = GenObjectKey(bucketName, prefix+TableMaxKeySuffix, TableMaxKeySuffix)
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		err := NewError(InTikvFatalError, "ListVersionedObjects TCBegin err", err)
		return listInfo, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		err := NewError(InTikvFatalError, "ListVersionedObjects Iter err", err)
		return listInfo, err
	}
	defer it.Close()
	for it.Valid() {
		v := it.Value()
		var objMeta Object
		err = helper.MsgPackUnMarshal(v, &objMeta)
		if err != nil {
			err := NewError(InTikvFatalError, "ListVersionedObjects MsgPackUnMarshal err", err)
			return listInfo, err
		}
		if previousNullObjectMeta != nil {
			if objMeta.Name != previousNullObjectMeta.Name {
				// fill in previous NullObject
				count++
				if count == maxKeys {
					listInfo.NextKeyMarker = previousNullObjectMeta.Name
					listInfo.NextVersionIdMarker = previousNullObjectMeta.VersionId
				}

				if count > maxKeys {
					previousNullObjectMeta = nil
					listInfo.IsTruncated = true
					exit = true
					break
				}

				o := ModifyMetaToVersionedObjectResult(*previousNullObjectMeta)
				listInfo.Objects = append(listInfo.Objects, o)
				previousNullObjectMeta = nil
			} else {
				// Compare which is the latest of null version object and versioned object
				var o datatype.VersionedObject
				nullIsLatest := previousNullObjectMeta.CreateTime > objMeta.CreateTime
				if nullIsLatest {
					o = ModifyMetaToVersionedObjectResult(*previousNullObjectMeta)
					previousNullObjectMeta = nil
				} else {
					o = ModifyMetaToVersionedObjectResult(objMeta)
				}

				count++
				if count == maxKeys {
					listInfo.NextKeyMarker = o.Key
					listInfo.NextVersionIdMarker = o.VersionId
				}

				if count > maxKeys {
					listInfo.IsTruncated = true
					exit = true
					break
				}

				listInfo.Objects = append(listInfo.Objects, o)

				if !nullIsLatest {
					if err := it.Next(context.TODO()); err != nil && it.Valid() {
						err := NewError(InTikvFatalError, "ListVersionedObjects get next err", err)
						return listInfo, err
					}
					continue
				}
			}
		}

		//filter prefix by delimiter
		if delimiter != "" {
			subKey := strings.TrimPrefix(objMeta.Name, prefix)
			sp := strings.SplitN(subKey, delimiter, 2)
			if len(sp) == 2 {
				prefixKey := prefix + sp[0] + delimiter
				if prefixKey == marker {
					startKey = GenObjectKey(bucketName, AddEndByteValue(prefixKey), NullVersion)
					it.Close()
					it, err = tx.Iter(context.TODO(), startKey, endKey)
					if err != nil {
						err := NewError(InTikvFatalError, "ListVersionedObjects Iter err", err)
						return listInfo, err
					}
					continue
				}

				if _, ok := commonPrefixes[prefixKey]; !ok {
					count++
					if count == maxKeys {
						listInfo.NextKeyMarker = prefixKey
						listInfo.NextVersionIdMarker = objMeta.VersionId
					}
					if count > maxKeys {
						listInfo.IsTruncated = true
						exit = true
						break
					}
					commonPrefixes[prefixKey] = nil
				}

				startKey = GenObjectKey(bucketName, AddEndByteValue(prefixKey), NullVersion)
				it.Close()
				it, err = tx.Iter(context.TODO(), startKey, endKey)
				if err != nil {
					err := NewError(InTikvFatalError, "ListVersionedObjects Iter err", err)
					return listInfo, err
				}
				continue
			}
		}

		if objMeta.VersionId == NullVersion {
			previousNullObjectMeta = &objMeta
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				err := NewError(InTikvFatalError, "ListVersionedObjects get next err", err)
				return listInfo, err
			}
			continue
		}

		o := ModifyMetaToVersionedObjectResult(objMeta)

		count++
		if count == maxKeys {
			listInfo.NextKeyMarker = o.Key
			listInfo.NextVersionIdMarker = o.VersionId
		}

		if count > maxKeys {
			listInfo.IsTruncated = true
			break
		}
		listInfo.Objects = append(listInfo.Objects, o)
		if err := it.Next(context.TODO()); err != nil && it.Valid() {
			err := NewError(InTikvFatalError, "ListVersionedObjects get next err", err)
			return listInfo, err
		}
	}

	if previousNullObjectMeta != nil {
		o := ModifyMetaToVersionedObjectResult(*previousNullObjectMeta)

		count++
		if count == maxKeys {
			listInfo.NextKeyMarker = o.Key
			listInfo.NextVersionIdMarker = o.VersionId
		}

		if count > maxKeys {
			listInfo.IsTruncated = true
			listInfo.Prefixes = helper.Keys(commonPrefixes)
			return listInfo, nil
		}
		listInfo.Objects = append(listInfo.Objects, o)
	}

	listInfo.Prefixes = helper.Keys(commonPrefixes)
	return
}

func (c *TiKVClient) UpdateUsage(bucketName string, size int64, tx Tx) error {
	if !helper.CONFIG.PiggybackUpdateUsage {
		return nil
	}

	// TODO : finished

	return nil
}

func (c *TiKVClient) IsEmptyBucket(bucket *Bucket) (isEmpty bool, err error) {
	if bucket.Versioning == datatype.BucketVersioningDisabled {
		listInfo, err := c.ListObjects(bucket.Name, "", "", "", 1)
		if err != nil {
			return false, err
		}
		if len(listInfo.Objects) != 0 || len(listInfo.Prefixes) != 0 {
			return false, nil
		}
	} else {
		listInfo, err := c.ListVersionedObjects(bucket.Name, "", "", "", "", 1)
		if err != nil {
			return false, err
		}
		if len(listInfo.Objects) != 0 || len(listInfo.Prefixes) != 0 {
			return false, nil
		}
	}

	// Check if object part is empty
	result, err := c.ListMultipartUploads(bucket.Name, "", "", "", "", "", 1)
	if err != nil {
		return false, err
	}
	if len(result.Uploads) != 0 {
		return false, nil
	}
	return true, nil
}
