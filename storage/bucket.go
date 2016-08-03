package storage

import (
	"git.letv.cn/yig/yig/iam"
	"git.letv.cn/yig/yig/meta"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
	"time"
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
			return meta.BucketExists{Bucket: bucket}
		}
		if string(b.Cells[0].Value) == credential.UserId {
			return meta.BucketExistsAndOwned{Bucket: bucket}
		} else {
			return meta.BucketExists{Bucket: bucket}
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
		err = meta.BucketAccessForbidden{Bucket: bucketName}
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
		return meta.BucketAccessForbidden{Bucket: bucketName}
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

	return
}
