package storage

import (
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/iam"
	"git.letv.cn/yig/yig/meta"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
	"time"
	"bytes"
	"encoding/binary"
	"strings"
	"github.com/tsuna/gohbase/filter"
)

const (
	CREATE_TIME_LAYOUT = "2006-01-02T15:04:05.000Z"
)

func (yig *YigStorage) MakeBucket(bucket string, credential iam.Credential) error {
	now := time.Now().UTC().Format(CREATE_TIME_LAYOUT)
	values := map[string]map[string][]byte{
		meta.BUCKET_COLUMN_FAMILY: map[string][]byte{
			"CORS":       []byte{}, // TODO
			"UID":        []byte(credential.UserId),
			"ACL":        []byte{}, // TODO
			"createTime": []byte(now),
		},
	}
	put, err := hrpc.NewPutStr(context.Background(), meta.BUCKET_TABLE, bucket, values)
	if err != nil {
		yig.Logger.Println("Error making hbase put: ", err)
		return err
	}
	processed, err := yig.MetaStorage.Hbase.CheckAndPut(put, meta.BUCKET_COLUMN_FAMILY,
		"UID", []byte{})
	if err != nil {
		yig.Logger.Println("Error making hbase checkandput: ", err)
		return err
	}
	if !processed { // bucket already exists, return accurate message
		family := map[string][]string{meta.BUCKET_COLUMN_FAMILY: []string{"UID"}}
		get, err := hrpc.NewGetStr(context.Background(), meta.BUCKET_TABLE, bucket,
			hrpc.Families(family))
		if err != nil {
			yig.Logger.Println("Error making hbase get: ", err)
			return err
		}
		b, err := yig.MetaStorage.Hbase.Get(get)
		if err != nil {
			yig.Logger.Println("Error get bucket: ", bucket, "with error: ", err)
			return ErrBucketAlreadyExists
		}
		if string(b.Cells[0].Value) == credential.UserId {
			return ErrBucketAlreadyOwnedByYou
		} else {
			return ErrBucketAlreadyExists
		}
	}
	err = yig.MetaStorage.AddBucketForUser(bucket, credential.UserId)
	if err != nil { // roll back bucket table, i.e. remove inserted bucket
		yig.Logger.Println("Error AddBucketForUser: ", err)
		del, err := hrpc.NewDelStr(context.Background(), meta.BUCKET_TABLE, bucket, values)
		if err != nil {
			yig.Logger.Println("Error making hbase del: ", err)
			yig.Logger.Println("Leaving junk bucket unremoved: ", bucket)
			return err
		}
		_, err = yig.MetaStorage.Hbase.Delete(del)
		if err != nil {
			yig.Logger.Println("Error deleting: ", err)
			yig.Logger.Println("Leaving junk bucket unremoved: ", bucket)
			return err
		}
	}
	return err
}

func (yig *YigStorage) GetBucketInfo(bucketName string,
	credential iam.Credential) (bucketInfo meta.BucketInfo, err error) {
	bucket, err := yig.MetaStorage.GetBucketInfo(bucketName)
	if err != nil {
		return
	}
	if bucket.OwnerId != credential.UserId {
		err = ErrBucketAccessForbidden
		return
		// TODO validate bucket policy
	}
	bucketInfo.Name = bucket.Name
	bucketInfo.Created = bucket.CreateTime
	return
}

func (yig *YigStorage) ListBuckets(credential iam.Credential) (buckets []meta.BucketInfo,
	err error) {
	bucketNames, err := yig.MetaStorage.GetUserBuckets(credential.UserId)
	if err != nil {
		return
	}
	for _, bucketName := range bucketNames {
		bucket, err := yig.MetaStorage.GetBucketInfo(bucketName)
		if err != nil {
			return buckets, err
		}
		buckets = append(buckets, meta.BucketInfo{
			Name:    bucket.Name,
			Created: bucket.CreateTime,
		})
	}
	return
}

func (yig *YigStorage) DeleteBucket(bucketName string, credential iam.Credential) error {
	bucket, err := yig.MetaStorage.GetBucketInfo(bucketName)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
		// TODO validate bucket policy
	}
	// TODO validate bucket is empty

	values := map[string]map[string][]byte{
		meta.BUCKET_COLUMN_FAMILY: map[string][]byte{},
	}
	deleteRequest, err := hrpc.NewDelStr(context.Background(), meta.BUCKET_TABLE, bucketName, values)
	if err != nil {
		return err
	}
	_, err = yig.MetaStorage.Hbase.Delete(deleteRequest)
	if err != nil {
		return err
	}

	err = yig.MetaStorage.RemoveBucketForUser(bucketName, credential.UserId)
	if err != nil { // roll back bucket table, i.e. re-add removed bucket entry
		values := map[string]map[string][]byte{
			meta.BUCKET_COLUMN_FAMILY: map[string][]byte{
				"CORS":       []byte(bucket.CORS),
				"UID":        []byte(bucket.OwnerId),
				"ACL":        []byte(bucket.ACL),
				"createTime": []byte(bucket.CreateTime),
			},
		}
		put, err := hrpc.NewPutStr(context.Background(), meta.BUCKET_TABLE, bucketName, values)
		if err != nil {
			yig.Logger.Println("Error making hbase put: ", err)
			yig.Logger.Println("Inconsistent data: bucket ", bucketName,
				"should be removed for user ", credential.UserId)
			return err
		}
		_, err = yig.MetaStorage.Hbase.Put(put)
		if err != nil {
			yig.Logger.Println("Error making hbase put: ", err)
			yig.Logger.Println("Inconsistent data: bucket ", bucketName,
				"should be removed for user ", credential.UserId)
			return err
		}
	}
	return nil
}

func (yig *YigStorage) ListObjects(bucket, prefix, marker, delimiter string,
	maxKeys int) (result meta.ListObjectsInfo, err error) {

	var prefixRowkey bytes.Buffer
	prefixRowkey.WriteString(bucket)
	err = binary.Write(&prefixRowkey, binary.BigEndian, uint16(strings.Count(prefix, "/")))
	if err != nil {
		return
	}
	startRowkey := bytes.NewBuffer(prefixRowkey.Bytes())
	prefixRowkey.WriteString(prefix)
	startRowkey.WriteString(marker)

	filter := filter.NewPrefixFilter(prefixRowkey)
	scanRequest, err := hrpc.NewScanRangeStr(context.Background(), meta.OBJECT_TABLE,
		// scan for max+1 rows to determine if results are truncated
		string(startRowkey), "", hrpc.Filters(filter), hrpc.NumberOfRows(maxKeys+1))
	if err != nil {
		return
	}
	scanResponse, err := yig.MetaStorage.Hbase.Scan(scanRequest)
	if err != nil {
		return
	}
	if len(scanResponse) > maxKeys {
		result.IsTruncated = true
		nextObject, err := meta.ObjectFromResponse(scanResponse[maxKeys], bucket)
		if err != nil {
			return
		}
		result.NextMarker = nextObject.Name
		scanResponse = scanResponse[:maxKeys]
	}
	var objects []meta.Object
	for _, row := range scanResponse {
		o, err := meta.ObjectFromResponse(row, bucket)
		if err != nil {
			return
		}
		objects = append(objects, o)
		// TODO prefix support
		// - add prefix when create new objects
		// - handle those prefix when listing
		// prefixes end with "/" and have depth as if the trailing "/" is removed
	}
	result.Objects = objects
	return
}
