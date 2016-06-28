package storage

import (
	"io"
	"git.letv.cn/yig/yig/minio/datatype"
)

func GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	return
}

func GetObjectInfo(bucket, object string) (objInfo datatype.ObjectInfo, err error) {
	return
}

func PutObject(bucket, object string, size int64, data io.Reader, metadata map[string]string) (md5 string, err error) {
	return
}

func DeleteObject(bucket, object string) error {
	return
}