package storage

import (
	"git.letv.cn/yig/yig/minio"
)

func MakeBucket(bucket string) error {
	return
}

func GetBucketInfo(bucket string) (bucketInfo minio.BucketInfo, err error) {
	return
}

func ListBuckets() (buckets []minio.BucketInfo, err error) {
	return
}

func DeleteBucket(bucket string) error {
	return
}

func ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListObjectsInfo, err error) {
	return
}