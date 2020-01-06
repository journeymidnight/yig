package tikvclient

import (
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/meta/types"
)

//object
func (c *TiKVClient) GetObject(bucketName, objectName, version string) (object *Object, err error) {
	return nil, nil
}
func (c *TiKVClient) GetAllObject(bucketName, objectName, version string) (object []*Object, err error) {
	return nil, nil
}
func (c *TiKVClient) GetAllOldObjects(bucketName, objectName, latestVersion string) (object []*Object, err error) {
	return nil, nil
}
func (c *TiKVClient) PutObject(object *Object, tx DB) error             { return nil }
func (c *TiKVClient) PutObjectWithoutMultiPart(object *Object) error    { return nil }
func (c *TiKVClient) UpdateObject(object *Object, tx DB) (err error)    { return nil }
func (c *TiKVClient) UpdateObjectWithoutMultiPart(object *Object) error { return nil }
func (c *TiKVClient) UpdateAppendObject(object *Object, tx DB) error    { return nil }
func (c *TiKVClient) RenameObjectPart(object *Object, sourceObject string, tx DB) (err error) {
	return nil
}
func (c *TiKVClient) RenameObject(object *Object, sourceObject string, tx DB) (err error) { return nil }
func (c *TiKVClient) DeleteObject(object *Object, tx DB) error                            { return nil }
func (c *TiKVClient) UpdateObjectAcl(object *Object) error                                { return nil }
func (c *TiKVClient) UpdateObjectAttrs(object *Object) error                              { return nil }

//bucket
func (c *TiKVClient) GetBucket(bucketName string) (bucket *Bucket, err error) { return nil, nil }
func (c *TiKVClient) GetBuckets() (buckets []Bucket, err error)               { return nil, nil }
func (c *TiKVClient) PutBucket(bucket Bucket) error                           { return nil }
func (c *TiKVClient) PutNewBucket(bucket Bucket) error                        { return nil }
func (c *TiKVClient) DeleteBucket(bucket Bucket) error                        { return nil }
func (c *TiKVClient) ListObjects(bucketName, marker, verIdMarker, prefix, delimiter string, versioned bool,
	maxKeys int) (retObjects []*Object, prefixes []string, truncated bool, nextMarker, nextVerIdMarker string, err error) {
	return
}
func (c *TiKVClient) UpdateUsage(bucketName string, size int64, tx DB) error  { return nil }
func (c *TiKVClient) IsEmptyBucket(bucketName string) (exist bool, err error) { return false, nil }

//multipart
func (c *TiKVClient) GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error) {
	return
}
func (c *TiKVClient) CreateMultipart(multipart Multipart) (err error) { return nil }
func (c *TiKVClient) PutObjectPart(multipart *Multipart, part *Part, tx DB) (err error) {
	return nil
}
func (c *TiKVClient) DeleteMultipart(multipart *Multipart, tx DB) (err error) { return nil }
func (c *TiKVClient) ListMultipartUploads(bucketName, keyMarker, uploadIdMarker, prefix, delimiter, encodingType string, maxUploads int) (uploads []datatype.Upload, prefixs []string, isTruncated bool, nextKeyMarker, nextUploadIdMarker string, err error) {
	return
}

//lc
func (c *TiKVClient) PutBucketToLifeCycle(lifeCycle LifeCycle) error { return nil }
func (c *TiKVClient) RemoveBucketFromLifeCycle(bucket Bucket) error  { return nil }
func (c *TiKVClient) ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error) {
	return
}

//user
func (c *TiKVClient) GetUserBuckets(userId string) (buckets []string, err error) { return nil, nil }

//gc
func (c *TiKVClient) PutObjectToGarbageCollection(object *Object, tx DB) error { return nil }
func (c *TiKVClient) ScanGarbageCollection(limit int, startRowKey string) ([]GarbageCollection, error) {
	return nil, nil
}
func (c *TiKVClient) RemoveGarbageCollection(garbage GarbageCollection) error { return nil }
