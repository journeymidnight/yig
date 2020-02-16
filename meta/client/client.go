package client

import (
	. "database/sql/driver"

	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/meta/types"
)

//DB Client Interface
type Client interface {
	//Transaction
	NewTrans() (tx Tx, err error)
	AbortTrans(tx Tx) error
	CommitTrans(tx Tx) error
	//object
	GetObject(bucketName, objectName, version string) (object *Object, err error)
	GetLatestVersionedObject(bucketName, objectName string) (object *Object, err error)
	PutObject(object *Object, multipart *Multipart, updateUsage bool) error
	PutObjectWithoutMultiPart(object *Object) error
	UpdateObject(object *Object, multipart *Multipart, updateUsage bool) (err error)
	UpdateObjectWithoutMultiPart(object *Object) error
	PutVersionedObject(object *Object, multipart *Multipart, updateUsage bool) error
	UpdateAppendObject(object *Object) error
	RenameObject(object *Object, sourceObject string) (err error)
	DeleteObject(object *Object, tx Tx) error
	DeleteVersionedObject(object *Object, tx Tx) error
	UpdateObjectAcl(object *Object) error
	UpdateObjectAttrs(object *Object) error
	//bucket
	GetBucket(bucketName string) (bucket *Bucket, err error)
	GetBuckets() (buckets []Bucket, err error)
	PutBucket(bucket Bucket) error
	PutNewBucket(bucket Bucket) error
	DeleteBucket(bucket Bucket) error
	ListObjects(bucketName, marker, prefix, delimiter string, maxKeys int) (listInfo ListObjectsInfo, err error)
	ListLatestObjects(bucketName, marker, prefix, delimiter string, maxKeys int) (listInfo ListObjectsInfo, err error)
	ListVersionedObjects(bucketName, marker, verIdMarker, prefix, delimiter string, maxKeys int) (listInfo VersionedListObjectsInfo, err error)
	UpdateUsage(bucketName string, size int64, tx Tx) error
	IsEmptyBucket(bucketName string) (isEmpty bool, err error)

	//multipart
	GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error)
	CreateMultipart(multipart Multipart) (err error)
	PutObjectPart(multipart *Multipart, part *Part) (err error)
	DeleteMultipart(multipart *Multipart, tx Tx) (err error)
	ListMultipartUploads(bucketName, keyMarker, uploadIdMarker, prefix, delimiter, encodingType string, maxUploads int) (result datatype.ListMultipartUploadsResponse, err error)

	//cluster
	GetClusters() (cluster []Cluster, err error)
	//lc
	PutBucketToLifeCycle(lifeCycle LifeCycle) error
	RemoveBucketFromLifeCycle(bucket Bucket) error
	ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error)
	//user
	GetUserBuckets(userId string) (buckets []string, err error)
	//gc
	PutObjectToGarbageCollection(object *Object, tx Tx) error
	ScanGarbageCollection(limit int) ([]GarbageCollection, error)
	RemoveGarbageCollection(garbage GarbageCollection) error
}
