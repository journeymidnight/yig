package client

import (
	. "database/sql/driver"

	"github.com/journeymidnight/yig/meta/common"

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
	GetObject(bucketName, objectName, version string, tx Tx) (object *Object, err error)
	GetLatestObjectVersion(bucketName, objectName string) (object *Object, err error)
	PutObject(object *Object, multipart *Multipart, updateUsage bool) error
	AppendObject(object *Object, updateUsage bool) error
	UpdateObject(object *Object, multipart *Multipart, updateUsage bool, tx Tx) (err error)
	UpdateAppendObject(object *Object) error
	MigrateObject(object *Object) error
	ListHotObjects(marker string, maxKeys int) (listInfo ListHotObjectsInfo, err error)
	RemoveHotObject(object *Object, tx Tx) error
	RenameObject(object *Object, sourceObject string) (err error)
	ReplaceObjectMetas(object *Object, tx Tx) (err error)
	DeleteObject(object *Object, tx Tx) error
	DeleteObjectPart(object *Object, tx Tx) error
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
	IsEmptyBucket(bucket *Bucket) (isEmpty bool, err error)

	//multipart
	GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error)
	CreateMultipart(multipart Multipart) (err error)
	PutObjectPart(multipart *Multipart, part *Part) (deltaSize int64, err error)
	DeleteMultipart(multipart *Multipart, tx Tx) (err error)
	ListMultipartUploads(bucketName, keyMarker, uploadIdMarker, prefix, delimiter, encodingType string, maxUploads int) (result datatype.ListMultipartUploadsResponse, err error)

	//cluster
	GetClusters() (cluster []Cluster, err error)

	//lc
	PutBucketToLifeCycle(bucket Bucket, lifeCycle LifeCycle) error
	GetBucketLifeCycle(bucket Bucket) (lifeCycle *LifeCycle, err error)
	RemoveBucketFromLifeCycle(bucket Bucket) error
	ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error)

	//user
	GetUserBuckets(userId string) (buckets []string, err error)

	//freezer
	CreateFreezer(freezer *Freezer) (err error)
	GetFreezer(bucketName, objectName, version string) (freezer *Freezer, err error)
	GetFreezerStatus(bucketName, objectName, version string) (freezer *Freezer, err error)
	UpdateFreezerDate(bucketName, objectName, version string, lifetime int) (err error)
	DeleteFreezer(bucketName, objectName, versionId string, objectType ObjectType, createTime uint64, tx Tx) (err error)
	ListFreezersWithStatus(maxKeys int, status common.RestoreStatus) (retFreezers []Freezer, err error)
	PutFreezer(freezer *Freezer, status common.RestoreStatus, tx Tx) (err error)
	UpdateFreezerStatus(bucketName, objectName, version string, status, statusSetting common.RestoreStatus) (err error)

	//qos
	GetAllUserQos() (userQos map[string]UserQos, err error)
	GetAllUserBuckets() (bucketUser map[string]string, err error)
}
