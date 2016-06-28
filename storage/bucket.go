package storage

import (
	"git.letv.cn/yig/yig/minio/datatype"
)

func (yig *YigStorage) MakeBucket(bucket string) error {
	return nil
}

func (yig *YigStorage) GetBucketInfo(bucket string) (bucketInfo datatype.BucketInfo, err error) {
	return
}

func (yig *YigStorage) ListBuckets() (buckets []datatype.BucketInfo, err error) {
	return
}

func (yig *YigStorage) DeleteBucket(bucket string) error {
	return nil
}

func (yig *YigStorage) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result datatype.ListObjectsInfo, err error) {
	return
}
