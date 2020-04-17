package tikvclient

import (
	"context"
	. "database/sql/driver"
	"math"
	"strings"

	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/tikv/client-go/key"
	"github.com/tikv/client-go/txnkv/kv"
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
		var info_o datatype.Object
		err = helper.MsgPackUnMarshal(v, &o)
		if err != nil {
			return info, err
		}
		info_o.Key = o.Name
		info_o.Owner = datatype.Owner{ID: o.OwnerId}
		info_o.ETag = o.Etag
		info_o.LastModified = o.LastModifiedTime.UTC().Format(CREATE_TIME_LAYOUT)
		info_o.Size = o.Size
		info_o.StorageClass = o.StorageClass.ToString()
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

func (c *TiKVClient) FindNextObject(it kv.Iterator, marker, prefix, delimiter string,
	commonPrefixes map[string]interface{}) (o Object, isPrefix bool, err error) {
	for it.Valid() {
		k, v := string(it.Key()[:]), it.Value()
		// extract object key
		objKey := strings.Split(k, TableSeparator)[1]
		if objKey == marker {
			it.Next(context.TODO())
			continue
		}
		if objKey == marker {
			it.Next(context.TODO())
			continue
		}
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
					o.Name = prefixKey
					return o, true, nil
				}
				it.Next(context.TODO())
				continue
			}
		}

		err = helper.MsgPackUnMarshal(v, &o)
		if err != nil {
			return o, false, err
		}
		return o, false, nil
	}
	return o, false, nil
}

func (c *TiKVClient) ListLatestObjects(bucketName, marker, prefix, delimiter string, maxKeys int) (info ListObjectsInfo, err error) {
	startKey := genObjectKey(bucketName, marker, NullVersion)
	endKey := genObjectKey(bucketName, TableMaxKeySuffix, NullVersion)

	startVerKey := genObjectKey(bucketName, marker, TableMinKeySuffix)
	endVerKey := genObjectKey(bucketName, TableMaxKeySuffix, TableMaxKeySuffix)

	txNull, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return info, err
	}
	itNull, err := txNull.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		return info, err
	}
	defer itNull.Close()

	txVer, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return info, err
	}
	itVer, err := txVer.Iter(context.TODO(), key.Key(startVerKey), key.Key(endVerKey))
	if err != nil {
		return info, err
	}
	defer itVer.Close()

	commonPrefixes := make(map[string]interface{})
	count := 0

	var nNext, vNext = true, true
	var verMarker = marker
	var currentNullObj, currentVerObj Object
	var isNullPrefix, isVerPrefix bool
	for {
		var o *Object
		if nNext {
			currentNullObj, isNullPrefix, err = c.FindNextObject(itNull, marker, prefix, delimiter, commonPrefixes)
			if err != nil {
				return info, err
			}
			itNull.Next(context.TODO())
			marker = currentNullObj.Name
		}
		if vNext {
			currentVerObj, isVerPrefix, err = c.FindNextObject(itVer, verMarker, prefix, delimiter, commonPrefixes)
			if err != nil {
				return info, err
			}
			itVer.Next(context.TODO())
			verMarker = currentVerObj.Name
		}

		// Pick latest obj
		if currentVerObj.Name == "" && currentNullObj.Name == "" {
			return
		} else if currentVerObj.Name == "" {
			o = &currentNullObj
			nNext = false
		} else if currentNullObj.Name == "" {
			o = &currentVerObj
			vNext = false
		} else {
			// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
			r := strings.Compare(currentVerObj.Name, currentNullObj.Name)
			if r < 0 {
				o = &currentVerObj
				vNext = false
			} else if r > 0 {
				o = &currentNullObj
				vNext = false
			} else {
				if isNullPrefix && isVerPrefix {
					o = &currentNullObj
					vNext, nNext = true, true
				}
				if currentNullObj.LastModifiedTime.After(currentVerObj.LastModifiedTime) {
					o = &currentNullObj
					nNext = false
				} else {
					o = &currentVerObj
					vNext = false
				}
			}
		}

		// is prefix
		if strings.HasSuffix(o.Name, delimiter) {
			prefixKey := o.Name
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
			continue
		}

		if o.DeleteMarker {
			continue
		}

		var info_o datatype.Object
		info_o.Key = o.Name
		info_o.Owner = datatype.Owner{ID: o.OwnerId}
		info_o.ETag = o.Etag
		info_o.LastModified = o.LastModifiedTime.UTC().Format(CREATE_TIME_LAYOUT)
		info_o.Size = o.Size
		info_o.StorageClass = o.StorageClass.ToString()
		count++
		if count == maxKeys {
			info.NextMarker = o.Name
		}
		if count > maxKeys {
			info.IsTruncated = true
			break
		}
		info.Objects = append(info.Objects, info_o)
	}
	info.Prefixes = helper.Keys(commonPrefixes)
	return
}

func (c *TiKVClient) FindNextVersionedObject(itVer kv.Iterator, marker, verIdMarker, prefix, delimiter string,
	commonPrefixes map[string]interface{}) (o Object, isPrefix bool, err error) {
	for itVer.Valid() {
		k, v := string(itVer.Key()[:]), itVer.Value()
		// extract object key
		sp := strings.Split(k, TableSeparator)
		if len(sp) != 4 {
			err = ErrInvalidObjectName
			return
		}
		objKey := sp[2]
		verId := sp[3]

		if objKey == marker && verId == verIdMarker {
			itVer.Next(context.TODO())
			continue
		}
		if !strings.HasPrefix(objKey, prefix) {
			itVer.Next(context.TODO())
			continue
		}

		if delimiter != "" {
			subKey := strings.TrimPrefix(objKey, prefix)
			sp := strings.SplitN(subKey, delimiter, 2)
			if len(sp) == 2 {
				prefixKey := prefix + sp[0] + delimiter
				if _, ok := commonPrefixes[prefixKey]; !ok && prefixKey != marker {
					o.Name = prefixKey
					return o, true, nil
				}
				itVer.Next(context.TODO())
				continue
			}
		}

		err = helper.MsgPackUnMarshal(v, &o)
		if err != nil {
			return o, false, err
		}
		return o, false, nil
	}
	return o, false, nil
}

func (c *TiKVClient) ListVersionedObjects(bucketName, marker, verIdMarker, prefix, delimiter string,
	maxKeys int) (info VersionedListObjectsInfo, err error) {
	startKey := genObjectKey(bucketName, marker, NullVersion)
	endKey := genObjectKey(bucketName, TableMaxKeySuffix, NullVersion)

	txNull, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return info, err
	}
	itNull, err := txNull.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		return info, err
	}
	defer itNull.Close()

	if marker != "" && verIdMarker == NullVersion {
		o, err := c.GetObject(bucketName, marker, NullVersion)
		if err != nil {
			if err == ErrNoSuchKey {
				return info, nil
			}
			return info, err
		}
		verIdMarker = o.GenVersionId(datatype.BucketVersioningEnabled)
	}

	startVerKey := genObjectKey(bucketName, marker, verIdMarker)
	endVerKey := genObjectKey(bucketName, TableMaxKeySuffix, TableMaxKeySuffix)

	txVer, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return info, err
	}
	itVer, err := txVer.Iter(context.TODO(), key.Key(startVerKey), key.Key(endVerKey))
	if err != nil {
		return info, err
	}
	defer itVer.Close()

	var nNext, vNext = true, true
	var currentNullObj, currentVerObj Object
	var isNullPrefix, isVerPrefix bool
	commonPrefixes := make(map[string]interface{})
	count := 0

	for {
		var o *Object
		if nNext {
			currentNullObj, isNullPrefix, err = c.FindNextObject(itNull, marker, prefix, delimiter, commonPrefixes)
			if err != nil {
				return info, err
			}
			if currentNullObj.Name != "" {
				itNull.Next(context.TODO())
			}
		}
		if vNext {
			currentVerObj, isVerPrefix, err = c.FindNextVersionedObject(itVer, marker, verIdMarker, prefix, delimiter, commonPrefixes)
			if err != nil {
				return info, err
			}
			if currentVerObj.Name != "" {
				itVer.Next(context.TODO())
			}
		}

		// Pick obj
		if currentVerObj.Name == "" && currentNullObj.Name == "" {
			return
		} else if currentVerObj.Name == "" {
			o = &currentNullObj
			vNext = false
		} else if currentNullObj.Name == "" {
			o = &currentVerObj
			nNext = false
		} else {
			// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
			r := strings.Compare(currentVerObj.Name, currentNullObj.Name)
			if r < 0 {
				o = &currentVerObj
				vNext = true
				nNext = false
			} else if r > 0 {
				o = &currentNullObj
				nNext = true
				vNext = false
			} else {
				if isNullPrefix && isVerPrefix {
					o = &currentNullObj
					vNext, nNext = true, true
				}
				if currentNullObj.LastModifiedTime.After(currentVerObj.LastModifiedTime) {
					o = &currentNullObj
					nNext = true
					vNext = false
				} else {
					o = &currentVerObj
					vNext = true
					nNext = false
				}
			}
		}

		// is prefix
		if strings.HasSuffix(o.Name, delimiter) {
			prefixKey := o.Name
			if _, ok := commonPrefixes[prefixKey]; !ok && prefixKey != marker {
				count++
				if count == maxKeys {
					info.NextKeyMarker = prefixKey
				}
				if count > maxKeys {
					info.IsTruncated = true
					break
				}
				commonPrefixes[prefixKey] = nil
			}
			continue
		}

		if o.DeleteMarker {
			continue
		}

		var info_o datatype.VersionedObject
		info_o.Key = o.Name
		info_o.Owner = datatype.Owner{ID: o.OwnerId}
		info_o.ETag = o.Etag
		info_o.LastModified = o.LastModifiedTime.UTC().Format(CREATE_TIME_LAYOUT)
		info_o.Size = o.Size
		info_o.StorageClass = o.StorageClass.ToString()
		info_o.VersionId = o.VersionId
		info_o.DeleteMarker = o.DeleteMarker
		count++
		if count == maxKeys {
			info.NextKeyMarker = o.Name
			info.NextVersionIdMarker = o.VersionId
		}
		if count > maxKeys {
			info.IsTruncated = true
			break
		}
		info.Objects = append(info.Objects, info_o)
	}
	info.Prefixes = helper.Keys(commonPrefixes)
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
	bucketName := bucket.Name
	bucketStartKey := GenKey(bucketName, TableMinKeySuffix)
	bucketEndKey := GenKey(bucketName, TableMaxKeySuffix)
	partStartKey := GenKey(TableObjectPartPrefix, bucketName, TableMinKeySuffix)
	partEndKey := GenKey(TableObjectPartPrefix, bucketName, TableMaxKeySuffix)
	r, err := c.TxScan(bucketStartKey, bucketEndKey, 1)
	if err != nil {
		return false, err
	}
	if len(r) > 0 {
		return false, nil
	}
	r, err = c.TxScan(partStartKey, partEndKey, 1)
	if err != nil {
		return false, err
	}
	if len(r) > 0 {
		return false, nil
	}
	return true, nil
}
