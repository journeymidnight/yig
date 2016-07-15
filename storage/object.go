package storage

import (
	"git.letv.cn/yig/yig/minio/datatype"
	"io"
)

func (yig *YigStorage) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	return
}

func (yig *YigStorage) GetObjectInfo(bucket, object string) (objInfo datatype.ObjectInfo, err error) {
	return
}

func (yig *YigStorage) PutObject(bucket, object string, size int64, data io.Reader, metadata map[string]string) (md5 string, err error) {
	return
}

func (yig *YigStorage) DeleteObject(bucket, object string) error {
	return nil
}
