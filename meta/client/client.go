package client

import (
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/meta/types"
)

//DB Client Interface
type Client interface {
	//Transaction
	NewTrans() (tx interface{}, err error)
	AbortTrans(tx interface{}) error
	CommitTrans(tx interface{}) error
	//object
	GetObject(bucketName, objectName, version string) (object *Object, err error)
	GetAllObject(bucketName, objectName, version string) (object []*Object, err error)
	PutObject(object *Object, tx interface{}) error
	UpdateAppendObject(object *Object) error
	UpdateObjectAttrs(object *Object) error
	DeleteObject(object *Object, tx interface{}) error
	UpdateObjectAcl(object *Object) error
	//bucket
	GetBucket(bucketName string) (bucket *Bucket, err error)
	GetBuckets() (buckets []Bucket, err error)
	PutBucket(bucket Bucket) error
	CheckAndPutBucket(bucket Bucket) (bool, error)
	DeleteBucket(bucket Bucket) error
	ListObjects(bucketName, marker, verIdMarker, prefix, delimiter string, versioned bool, maxKeys int) (retObjects []*Object, prefixes []string, truncated bool, nextMarker, nextVerIdMarker string, err error)
	UpdateUsage(bucketName string, size int64, tx interface{}) error

	//multipart
	GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error)
	CreateMultipart(multipart Multipart) (err error)
	PutObjectPart(multipart *Multipart, part *Part, tx interface{}) (err error)
	DeleteMultipart(multipart *Multipart, tx interface{}) (err error)
	ListMultipartUploads(bucketName, keyMarker, uploadIdMarker, prefix, delimiter, encodingType string, maxUploads int) (uploads []datatype.Upload, prefixs []string, isTruncated bool, nextKeyMarker, nextUploadIdMarker string, err error)
	//objmap
	GetObjectMap(bucketName, objectName string) (objMap *ObjMap, err error)
	PutObjectMap(objMap *ObjMap, tx interface{}) error
	DeleteObjectMap(objMap *ObjMap, tx interface{}) error
	//cluster
	GetCluster(fsid, pool string) (cluster Cluster, err error)
	//lc
	PutBucketToLifeCycle(lifeCycle LifeCycle) error
	RemoveBucketFromLifeCycle(bucket Bucket) error
	ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error)
	//user
	GetUserBuckets(userId string) (buckets []string, err error)
	AddBucketForUser(bucketName, userId string) (err error)
	RemoveBucketForUser(bucketName string, userId string) (err error)
	//gc
	PutObjectToGarbageCollection(object *Object, tx interface{}) error
	ScanGarbageCollection(limit int, startRowKey string) ([]GarbageCollection, error)
	RemoveGarbageCollection(garbage GarbageCollection) error
}
