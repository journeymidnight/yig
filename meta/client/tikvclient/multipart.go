package tikvclient

import (
	. "database/sql/driver"

	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/meta/types"
)

func genMultipartKey(bucketName, objectName, uploadId string) []byte {
	return GenKey(TableMultipartPrefix, bucketName, objectName, uploadId)
}

//multipart
func (c *TiKVClient) GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error) {
	return
}
func (c *TiKVClient) CreateMultipart(multipart Multipart) (err error) { return nil }
func (c *TiKVClient) PutObjectPart(multipart *Multipart, part *Part, tx Tx) (err error) {
	return nil
}
func (c *TiKVClient) DeleteMultipart(multipart *Multipart, tx Tx) (err error) { return nil }
func (c *TiKVClient) ListMultipartUploads(bucketName, keyMarker, uploadIdMarker, prefix, delimiter, encodingType string, maxUploads int) (uploads []datatype.Upload, prefixs []string, isTruncated bool, nextKeyMarker, nextUploadIdMarker string, err error) {
	return
}
