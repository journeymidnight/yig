package client

import (
	"database/sql"
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/meta/types"
)

//DB Client Interface
type Client interface {
	//Transaction
	NewTrans() (tx *sql.Tx, err error)
	AbortTrans(tx *sql.Tx) error
	CommitTrans(tx *sql.Tx) error
	//object
	GetObject(bucketName, objectName, version string) (object *Object, err error)
	GetAllObject(bucketName, objectName, version string) (object []*Object, err error)
	PutObject(object *Object, tx DB) error
	UpdateAppendObject(object *Object, tx DB) error
	RenameObjectPart(object *Object, sourceObject string, tx DB) (err error)
	RenameObject(object *Object, sourceObject string, tx DB) (err error)
	ReplaceObjectMetas(object *Object, tx DB) (err error)
	DeleteObject(object *Object, tx DB) error
	UpdateObject(object *Object, tx DB) (err error)
	UpdateObjectAcl(object *Object) error
	UpdateObjectAttrs(object *Object) error
	//bucket
	GetBucket(bucketName string) (bucket *Bucket, err error)
	GetBuckets() (buckets []Bucket, err error)
	PutBucket(bucket Bucket) error
	CheckAndPutBucket(bucket Bucket) (bool, error)
	DeleteBucket(bucket Bucket) error
	ListObjects(bucketName, marker, verIdMarker, prefix, delimiter string, versioned bool, maxKeys int) (retObjects []*Object, prefixes []string, truncated bool, nextMarker, nextVerIdMarker string, err error)
	UpdateUsage(bucketName string, size int64, tx DB) error

	//multipart
	GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error)
	CreateMultipart(multipart Multipart) (err error)
	PutObjectPart(multipart *Multipart, part *Part, tx DB) (err error)
	DeleteMultipart(multipart *Multipart, tx DB) (err error)
	ListMultipartUploads(bucketName, keyMarker, uploadIdMarker, prefix, delimiter, encodingType string, maxUploads int) (uploads []datatype.Upload, prefixs []string, isTruncated bool, nextKeyMarker, nextUploadIdMarker string, err error)
	//objmap
	GetObjectMap(bucketName, objectName string) (objMap *ObjMap, err error)
	PutObjectMap(objMap *ObjMap, tx DB) error
	DeleteObjectMap(objMap *ObjMap, tx DB) error
	//cluster
	GetClusters() (cluster []Cluster, err error)
	//lc
	PutBucketToLifeCycle(lifeCycle LifeCycle) error
	RemoveBucketFromLifeCycle(bucket Bucket) error
	ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error)
	//user
	GetUserBuckets(userId string) (buckets []string, err error)
	AddBucketForUser(bucketName, userId string) (err error)
	RemoveBucketForUser(bucketName string, userId string) (err error)
	//gc
	PutObjectToGarbageCollection(object *Object, tx DB) error
	PutFreezerToGarbageCollection(object *Freezer, tx DB) (err error)
	ScanGarbageCollection(limit int, startRowKey string) ([]GarbageCollection, error)
	RemoveGarbageCollection(garbage GarbageCollection) error
	//freezer
	CreateFreezer(freezer *Freezer) (err error)
	GetFreezer(bucketName, objectName, version string) (freezer *Freezer, err error)
	GetFreezerStatus(bucketName, objectName, version string) (freezer *Freezer, err error)
	UploadFreezerDate(bucketName, objectName string, lifetime int) (err error)
	DeleteFreezer(bucketName, objectName string, tx DB) (err error)
}
