package tikvclient

import (
	"context"
	. "database/sql/driver"
	"math"
	"strings"

	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/client"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/tikv/client-go/key"
)

// **Key**: b\{BucketName}
func genBucketKey(bucketName string) []byte {
	return GenKey(TableBucketPrefix, bucketName)
}

//bucket
func (c *TiKVClient) GetBucket(bucketName string) (*Bucket, error) {
	bucketKey := genBucketKey(bucketName)
	var b Bucket
	ok, err := c.TxGet(bucketKey, &b, nil)
	if err != nil {
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
			return nil, err
		}
		buckets = append(buckets, b)
	}
	return buckets, nil
}

func (c *TiKVClient) PutBucket(bucket Bucket) error {
	bucketKey := genBucketKey(bucket.Name)
	return c.TxPut(bucketKey, bucket)
}

func (c *TiKVClient) PutNewBucket(bucket Bucket) error {
	bucketKey := genBucketKey(bucket.Name)
	userBucketKey := genUserBucketKey(bucket.OwnerId, bucket.Name)
	existBucket, err := c.TxExist(bucketKey)
	if err != nil {
		return err
	}
	existUserBucket, err := c.TxExist(userBucketKey)
	if err != nil {
		return err
	}
	if existBucket && existUserBucket {
		return ErrBucketAlreadyExists
	}

	return c.TxPut(bucketKey, bucket, userBucketKey, 0)
}

func (c *TiKVClient) DeleteBucket(bucket Bucket) error {
	bucketKey := genBucketKey(bucket.Name)
	userBucketKey := genUserBucketKey(bucket.OwnerId, bucket.Name)
	lifeCycleKey := genLifecycleKey(bucket.Name)
	return c.TxDelete(bucketKey, userBucketKey, lifeCycleKey)
}

func (c *TiKVClient) ListObjects(bucketName, marker, prefix, delimiter string, maxKeys int) (listInfo ListObjectsInfo, err error) {
	var startVersion = TableMaxKeySuffix
	if prefix != "" {
		if marker == "" || strings.Compare(marker, prefix) < 0 {
			marker = prefix
			startVersion = NullVersion
		} else if !strings.HasPrefix(marker, prefix) && strings.Compare(marker, prefix) > 0 {
			return listInfo, err
		}
	}

	startKey := genObjectKey(bucketName, marker, startVersion)
	endKey := genObjectKey(bucketName, TableMaxKeySuffix, TableMaxKeySuffix)

	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return listInfo, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		return listInfo, err
	}
	defer it.Close()
	commonPrefixes := make(map[string]interface{})

	count := 0
	for it.Valid() {
		k, v := string(it.Key()), it.Value()
		if k == string(startKey) {
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				return listInfo, err
			}
			continue
		}
		// extract object key
		objKey := strings.SplitN(k, TableSeparator, 2)[1]
		if !strings.HasPrefix(objKey, prefix) {
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				return listInfo, err
			}
			break
		}
		if delimiter != "" {
			subKey := strings.TrimPrefix(objKey, prefix)
			sp := strings.SplitN(subKey, delimiter, 2)
			if len(sp) == 2 {
				prefixKey := prefix + sp[0] + delimiter
				if _, ok := commonPrefixes[prefixKey]; !ok && prefixKey != marker {
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
				if err := it.Next(context.TODO()); err != nil && it.Valid() {
					return listInfo, err
				}
				continue
			}
		}
		var o Object
		err = helper.MsgPackUnMarshal(v, &o)
		if err != nil {
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
			return listInfo, err
		}
	}
	listInfo.Prefixes = helper.Keys(commonPrefixes)
	return
}

func (c *TiKVClient) ListLatestObjects(bucketName, marker, prefix, delimiter string, maxKeys int) (listInfo ListObjectsInfo, err error) {
	var startVersion = TableMaxKeySuffix
	if prefix != "" {
		if marker == "" || strings.Compare(marker, prefix) < 0 {
			marker = prefix
			startVersion = NullVersion
		} else if !strings.HasPrefix(marker, prefix) && strings.Compare(marker, prefix) > 0 {
			return listInfo, err
		}
	}

	startKey := genObjectKey(bucketName, marker, startVersion)
	endKey := genObjectKey(bucketName, TableMaxKeySuffix, TableMaxKeySuffix)

	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return listInfo, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		return listInfo, err
	}
	defer it.Close()

	commonPrefixes := make(map[string]interface{})
	count := 0
	objectMap := make(map[string]interface{})
	var previousNullObjectMeta *Object
	for it.Valid() {
		k, v := string(it.Key()), it.Value()
		var objMeta Object
		err = helper.MsgPackUnMarshal(v, &objMeta)
		if err != nil {
			return listInfo, err
		}

		if previousNullObjectMeta != nil {
			var meta Object
			if objMeta.Name != previousNullObjectMeta.Name {
				meta = *previousNullObjectMeta
			} else {
				if objMeta.CreateTime > previousNullObjectMeta.CreateTime {
					meta = objMeta
				} else {
					meta = *previousNullObjectMeta
				}
			}

			if meta.DeleteMarker {
				objectMap[meta.Name] = nil
				if err := it.Next(context.TODO()); err != nil && it.Valid() {
					return listInfo, err
				}
				continue
			}

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
			objectMap[meta.Name] = nil
			listInfo.Objects = append(listInfo.Objects, o)

			// Compare once
			previousNullObjectMeta = nil
		}

		// extract object key
		keySp := strings.Split(k, TableSeparator)
		objKey := keySp[1]

		if objKey == marker {
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				return listInfo, err
			}
			continue
		}

		if _, ok := objectMap[objKey]; ok {
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				return listInfo, err
			}
			continue
		}

		if !strings.HasPrefix(objKey, prefix) {
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				return listInfo, err
			}
			break
		}

		// If delete marker, do continue
		if objMeta.DeleteMarker {
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				return listInfo, err
			}
			continue
		}

		if delimiter != "" {
			subKey := strings.TrimPrefix(objKey, prefix)
			sp := strings.SplitN(subKey, delimiter, 2)
			if len(sp) == 2 {
				prefixKey := prefix + sp[0] + delimiter
				if _, ok := commonPrefixes[prefixKey]; !ok && prefixKey != marker {
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
				if err := it.Next(context.TODO()); err != nil && it.Valid() {
					return listInfo, err
				}
				continue
			}
		}

		// null version object
		if len(keySp) == 2 {
			previousNullObjectMeta = &objMeta
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				return listInfo, err
			}
			continue
		} else {
			previousNullObjectMeta = nil
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
		objectMap[objMeta.Name] = nil
		listInfo.Objects = append(listInfo.Objects, o)
		if err := it.Next(context.TODO()); err != nil && it.Valid() {
			return listInfo, err
		}
	}

	if previousNullObjectMeta != nil {
		o := ModifyMetaToObjectResult(*previousNullObjectMeta)
		count++
		if count == maxKeys {
			listInfo.NextMarker = o.Key
		}

		if count > maxKeys {
			listInfo.IsTruncated = true
		}
		objectMap[o.Key] = nil
		listInfo.Objects = append(listInfo.Objects, o)

		previousNullObjectMeta = nil
	}
	listInfo.Prefixes = helper.Keys(commonPrefixes)
	return
}

func (c *TiKVClient) ListVersionedObjects(bucketName, marker, verIdMarker, prefix, delimiter string,
	maxKeys int) (listInfo VersionedListObjectsInfo, err error) {
	var startVersion = TableMaxKeySuffix
	var needCheckMarker = true
	if prefix != "" {
		if marker == "" || strings.Compare(marker, prefix) < 0 {
			marker = prefix
			startVersion = NullVersion
			needCheckMarker = false
		} else if !strings.HasPrefix(marker, prefix) && strings.Compare(marker, prefix) > 0 {
			return listInfo, err
		}
	}

	commonPrefixes := make(map[string]interface{})
	var count int
	var exit bool
	var previousNullObjectMeta *Object
	var startKey, endKey []byte

	isPrefixMarker := (delimiter != "" && strings.HasSuffix(marker, delimiter))
	if marker != "" && !isPrefixMarker && needCheckMarker {
		var needCompareNull = true
		var nullObjMeta *Object
		nullObjMeta, err = c.GetObject(bucketName, marker, NullVersion)
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

		startKey = genObjectKey(bucketName, marker, verIdMarker)
		endKey = genObjectKey(bucketName, marker, TableMaxKeySuffix)

		tx, err := c.TxnCli.Begin(context.TODO())
		if err != nil {
			return listInfo, err
		}
		it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
		if err != nil {
			return listInfo, err
		}
		defer it.Close()

		for it.Valid() {
			v := it.Value()
			var VerObjMeta Object
			err = helper.MsgPackUnMarshal(v, &VerObjMeta)
			if err != nil {
				return listInfo, err
			}
			if VerObjMeta.Name == marker && VerObjMeta.VersionId == verIdMarker {
				if err := it.Next(context.TODO()); err != nil && it.Valid() {
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
				return listInfo, err
			}
		}
	}

	if exit {
		return listInfo, nil
	}

	startKey = genObjectKey(bucketName, marker, startVersion)
	endKey = genObjectKey(bucketName, TableMaxKeySuffix, NullVersion)
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return listInfo, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		return listInfo, err
	}
	defer it.Close()
	for it.Valid() {
		v := it.Value()
		var objMeta Object
		err = helper.MsgPackUnMarshal(v, &objMeta)
		if err != nil {
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
						return listInfo, err
					}
					continue
				}
			}
		}

		if !strings.HasPrefix(objMeta.Name, prefix) {
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				return listInfo, err
			}
			continue
		}

		//filter prefix by delimiter
		if delimiter != "" {
			subKey := strings.TrimPrefix(objMeta.Name, prefix)
			sp := strings.SplitN(subKey, delimiter, 2)
			if len(sp) == 2 {
				prefixKey := prefix + sp[0] + delimiter
				if _, ok := commonPrefixes[prefixKey]; !ok && prefixKey != marker {
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
				if err := it.Next(context.TODO()); err != nil && it.Valid() {
					return listInfo, err
				}
				continue
			}
		}

		if objMeta.VersionId == NullVersion {
			previousNullObjectMeta = &objMeta
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
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

	bucketKey := genBucketKey(bucketName)
	var bucket Bucket
	ok, err := c.TxGet(bucketKey, &bucket, txn)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNoSuchBucket
	}

	userBucketKey := genUserBucketKey(bucket.OwnerId, bucket.Name)
	var usage int64

	ok, err = c.TxGet(userBucketKey, &usage, txn)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNoSuchBucket
	}
	usage += size

	v, err := helper.MsgPackMarshal(usage)
	if err != nil {
		return err
	}
	return txn.Set(userBucketKey, v)
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
