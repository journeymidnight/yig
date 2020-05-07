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

func genBucketKey(bucketName string) []byte {
	return GenKey(TableBucketPrefix, bucketName)
}

//bucket
func (c *TiKVClient) GetBucket(bucketName string) (*Bucket, error) {
	bucketKey := genBucketKey(bucketName)
	var b Bucket
	ok, err := c.TxGet(bucketKey, &b)
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
	kvs, err := c.TxScan(startKey, endKey, math.MaxInt64)
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
	lifeCycleKey := genLifecycleKey()
	return c.TxDelete(bucketKey, userBucketKey, lifeCycleKey)
}

func (c *TiKVClient) ListObjects(bucketName, marker, prefix, delimiter string, maxKeys int) (info ListObjectsInfo, err error) {
	startKey := genObjectKey(bucketName, marker, NullVersion)
	endKey := genObjectKey(bucketName, TableMaxKeySuffix, NullVersion)

	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return info, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		return info, err
	}
	defer it.Close()
	commonPrefixes := make(map[string]interface{})

	count := 0
	for it.Valid() {
		k, v := string(it.Key()[:]), it.Value()
		if k == string(startKey) {
			it.Next(context.TODO())
			continue
		}
		// extract object key
		objKey := strings.SplitN(k, TableSeparator, 2)[1]
		if !strings.HasPrefix(objKey, prefix) {
			it.Next(context.TODO())
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
						info.NextMarker = prefixKey
					}
					if count > maxKeys {
						info.IsTruncated = true
						break
					}
					commonPrefixes[prefixKey] = nil
				}
				it.Next(context.TODO())
				continue
			}
		}
		var o Object
		err = helper.MsgPackUnMarshal(v, &o)
		if err != nil {
			return info, err
		}

		info_o := ModifyMetaToObjectResult(o)
		count++
		if count == maxKeys {
			info.NextMarker = objKey
		}
		if count > maxKeys {
			info.IsTruncated = true
			break
		}
		info.Objects = append(info.Objects, info_o)
		it.Next(context.TODO())
	}
	info.Prefixes = helper.Keys(commonPrefixes)
	return
}

func (c *TiKVClient) ListLatestObjects(bucketName, marker, prefix, delimiter string, maxKeys int) (listInfo ListObjectsInfo, err error) {
	startKey := genObjectKey(bucketName, marker, TableMaxKeySuffix)
	endKey := genObjectKey(bucketName, TableMaxKeySuffix, NullVersion)

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
		k, v := string(it.Key()[:]), it.Value()
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
				it.Next(context.TODO())
				continue
			}

			previousNullObjectMeta = nil
			o := ModifyMetaToObjectResult(meta)

			count++
			if count == maxKeys {
				listInfo.NextMarker = o.Key
			}

			if count > maxKeys {
				listInfo.IsTruncated = true
				break
			}
			objectMap[meta.Name] = nil
			listInfo.Objects = append(listInfo.Objects, o)

			// Compare once

			if objMeta.Name == previousNullObjectMeta.Name {
				previousNullObjectMeta = nil
				it.Next(context.TODO())
				continue
			}

		}

		// extract object key
		keySp := strings.Split(k, TableSeparator)
		objKey := keySp[1]

		if objKey == marker {
			it.Next(context.TODO())
			continue
		}

		if _, ok := objectMap[objKey]; ok {
			it.Next(context.TODO())
			continue
		}

		if !strings.HasPrefix(objKey, prefix) {
			it.Next(context.TODO())
			continue
		}

		// If delete marker, do continue
		if objMeta.DeleteMarker {
			it.Next(context.TODO())
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
				it.Next(context.TODO())
				continue
			}
		}

		// null version object
		if len(keySp) == 2 {
			previousNullObjectMeta = &objMeta
			it.Next(context.TODO())
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
		it.Next(context.TODO())
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

	commonPrefixes := make(map[string]interface{})
	var count int
	var exit bool
	var previousNullObjectMeta *Object
	var startKey, endKey []byte

	isPrefixMarker := (delimiter != "" && strings.HasSuffix(marker, delimiter))
	if marker != "" && !isPrefixMarker {
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
			it.Next(context.TODO())
		}
	}

	if exit {
		return listInfo, nil
	}

	startKey = genObjectKey(bucketName, marker, TableMaxKeySuffix)
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
				previousNullObjectMeta = nil
				// fill in previous NullObject
				count++
				if count == maxKeys {
					listInfo.NextKeyMarker = previousNullObjectMeta.Name
					listInfo.NextVersionIdMarker = previousNullObjectMeta.VersionId
				}

				if count > maxKeys {
					listInfo.IsTruncated = true
					exit = true
					break
				}

				o := ModifyMetaToVersionedObjectResult(*previousNullObjectMeta)
				listInfo.Objects = append(listInfo.Objects, o)

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
					it.Next(context.TODO())
					continue
				}
			}
		}

		if !strings.HasPrefix(objMeta.Name, prefix) {
			it.Next(context.TODO())
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
				continue
			}
		}

		if objMeta.VersionId == NullVersion {
			previousNullObjectMeta = &objMeta
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
		it.Next(context.TODO())
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

	bucket, err := c.GetBucket(bucketName)
	if err != nil {
		return err
	}

	userBucketKey := genUserBucketKey(bucket.OwnerId, bucket.Name)
	var usage int64

	if tx == nil {
		ok, err := c.TxGet(userBucketKey, &usage)
		if err != nil {
			return err
		}
		if !ok {
			return ErrNoSuchBucket
		}
		usage += size
		return c.TxPut(userBucketKey, usage)
	}

	v, err := tx.(*TikvTx).tx.Get(context.TODO(), userBucketKey)
	if err != nil {
		return err
	}

	err = helper.MsgPackUnMarshal(v, &usage)
	if err != nil {
		return err
	}

	usage += size

	v, err = helper.MsgPackMarshal(usage)
	if err != nil {
		return err
	}
	return tx.(*TikvTx).tx.Set(userBucketKey, v)
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
