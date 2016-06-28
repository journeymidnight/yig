package storage

import (
	"io"
	"git.letv.cn/yig/yig/minio"
)

func ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartsInfo, err error) {
	return
}

func NewMultipartUpload(bucket, object string, metadata map[string]string) (uploadID string, err error) {
	return
}

func PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (md5 string, err error) {
	return
}

func ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result minio.ListPartsInfo, err error) {
	return
}

func AbortMultipartUpload(bucket, object, uploadID string) error {
	return
}

func CompleteMultipartUpload(bucket, object, uploadID string, uploadedParts []minio.CompletePart) (md5 string, err error) {
	return
}