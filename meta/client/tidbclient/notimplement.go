package tidbclient

import (
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) GetAllObject(bucketName, objectName, version string) (object []*Object, err error) {
	return
}
func (t *TidbClient) DeleteObject(object *Object) error {
	return ErrNotImplemented
}

func (t *TidbClient) DeleteBucket(bucket Bucket) error { return ErrNotImplemented }

func (t *TidbClient) GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error) {
	return
}
func (t *TidbClient) CreateMultipart(multipart Multipart) (err error)          { return }
func (t *TidbClient) PutObjectPart(multipart Multipart, part Part) (err error) { return }

func (t *TidbClient) DeleteMultipart(multipart Multipart) (err error) { return }

func (t *TidbClient) ListMultipartUploads(bucketName, keyMarker, uploadIdMarker, prefix, delimiter, encodingType string, maxUploads int) (uploads []datatype.Upload, prefixs []string, isTruncated bool, nextKeyMarker, nextUploadIdMarker string, err error) {
	return
}

//objmap
func (t *TidbClient) GetObjectMap(bucketName, objectName string) (objMap *ObjMap, err error) { return }

func (t *TidbClient) PutObjectMap(objMap *ObjMap) error { return ErrNotImplemented }

func (t *TidbClient) DeleteObjectMap(objMap *ObjMap) error {
	return ErrNotImplemented
}

//cluster
func (t *TidbClient) GetCluster(fsid, pool string) (cluster Cluster, err error) { return }

//lc
func (t *TidbClient) PutBucketToLifeCycle(lifeCycle LifeCycle) error {
	return ErrNotImplemented
}

func (t *TidbClient) RemoveBucketFromLifeCycle(bucket Bucket) error {
	return ErrNotImplemented
}

func (t *TidbClient) ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error) {
	return
}

//gc
func (t *TidbClient) PutObjectToGarbageCollection(object *Object) error {
	return ErrNotImplemented
}

func (t *TidbClient) ScanGarbageCollection(limit int, startRowKey string) ([]GarbageCollection, error) {
	return nil, ErrNotImplemented
}

func (t *TidbClient) RemoveGarbageCollection(garbage GarbageCollection) error {
	return ErrNotImplemented
}
