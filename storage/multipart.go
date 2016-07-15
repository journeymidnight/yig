package storage

import (
	"git.letv.cn/yig/yig/minio/datatype"
	"io"
)

func (yig *YigStorage) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result datatype.ListMultipartsInfo, err error) {
	return
}

func (yig *YigStorage) NewMultipartUpload(bucket, object string, metadata map[string]string) (uploadID string, err error) {
	return
}

func (yig *YigStorage) PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (md5 string, err error) {
	return
}

func (yig *YigStorage) ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result datatype.ListPartsInfo, err error) {
	return
}

func (yig *YigStorage) AbortMultipartUpload(bucket, object, uploadID string) error {
	return nil
}

func (yig *YigStorage) CompleteMultipartUpload(bucket, object, uploadID string, uploadedParts []datatype.CompletePart) (md5 string, err error) {
	return
}
