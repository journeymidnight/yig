package storage

import (
	"git.letv.cn/yig/yig/minio/datatype"
)

func MakeBucket(bucket string) error {
	return
}

func GetBucketInfo(bucket string) (bucketInfo datatype.BucketInfo, err error) {
	return
}

func ListBuckets() (buckets []datatype.BucketInfo, err error) {
	return
}

func DeleteBucket(bucket string) error {
	return
}

func ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result datatype.ListObjectsInfo, err error) {
	return
}