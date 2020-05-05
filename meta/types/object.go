package types

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/meta/common"
)

const NullVersion = "0"

type Object struct {
	Rowkey           []byte // Rowkey cache
	Name             string
	BucketName       string
	Location         string // which Ceph cluster this object locates
	Pool             string // which Ceph pool this object locates
	OwnerId          string
	Size             int64     // file size
	ObjectId         string    // object name in Ceph
	LastModifiedTime time.Time // in format "2006-01-02T15:04:05.000Z"
	Etag             string
	ContentType      string
	CustomAttributes map[string]string
	Parts            map[int]*Part
	PartsIndex       *SimpleIndex
	ACL              datatype.Acl
	NullVersion      bool   // if this entry has `null` version
	DeleteMarker     bool   // if this entry is a delete marker
	VersionId        string // version cache
	// type of Server Side Encryption, could be "SSE-KMS", "SSE-S3", "SSE-C"(custom), or ""(none),
	// KMS is not implemented yet
	SseType string
	// encryption key for SSE-S3, the key itself is encrypted with SSE_S3_MASTER_KEY,
	// in AES256-GCM
	EncryptionKey        []byte
	InitializationVector []byte
	// ObjectType include `Normal`, `Appendable`, 'Multipart'
	Type         ObjectType
	StorageClass StorageClass
	CreateTime   uint64 // Timestamp(nanosecond)
}

type ObjectType int

const (
	ObjectTypeNormal     ObjectType = 0
	ObjectTypeAppendable ObjectType = 1
	ObjectTypeMultipart  ObjectType = 2
)

func (o *Object) ObjectTypeToString() string {
	switch ObjectType(o.Type) {
	case ObjectTypeNormal:
		return "Normal"
	case ObjectTypeAppendable:
		return "Appendable"
	case ObjectTypeMultipart:
		return "Multipart"
	default:
		return "Unknown"
	}
}

func (o *Object) String() (s string) {
	s += "Name: " + o.Name + "\t"
	s += "Location: " + o.Location + "\t"
	s += "Pool: " + o.Pool + "\t"
	s += "Object ID: " + o.ObjectId + "\t"
	s += "Last Modified Time: " + o.LastModifiedTime.Format(CREATE_TIME_LAYOUT) + "\t"
	s += "Version: " + o.VersionId + "\t"
	s += "Type: " + o.ObjectTypeToString() + "\t"
	s += "StorageClass: " + o.StorageClass.ToString() + "\t"
	for n, part := range o.Parts {
		s += fmt.Sprintf("Part %d ObjectID: %s\t", n, part.ObjectId)
	}
	return s
}

func (o *Object) GenVersionId(bucketVersionType datatype.BucketVersioningType) string {
	if bucketVersionType != datatype.BucketVersioningEnabled {
		return NullVersion
	}

	return strconv.FormatUint(math.MaxUint64-o.CreateTime, 10)
}

//Tidb related function

func (o *Object) GetCreateSql() (string, []interface{}) {

	customAttributes, _ := json.Marshal(o.CustomAttributes)
	acl, _ := json.Marshal(o.ACL)
	lastModifiedTime := o.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := "insert into objects(bucketname,name,version,location,pool,ownerid,size,objectid,lastmodifiedtime,etag," +
		"contenttype,customattributes,acl,nullversion,deletemarker,ssetype,encryptionkey,initializationvector,type,storageclass,createtime) " +
		"values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	args := []interface{}{o.BucketName, o.Name, o.VersionId, o.Location, o.Pool, o.OwnerId, o.Size, o.ObjectId,
		lastModifiedTime, o.Etag, o.ContentType, customAttributes, acl, o.NullVersion, o.DeleteMarker,
		o.SseType, o.EncryptionKey, o.InitializationVector, o.Type, o.StorageClass, o.LastModifiedTime.UnixNano()}
	return sql, args
}

func (o *Object) GetCreateHotSql() (string, []interface{}) {

	customAttributes, _ := json.Marshal(o.CustomAttributes)
	acl, _ := json.Marshal(o.ACL)
	lastModifiedTime := o.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := "insert into hotobjects(bucketname,name,version,location,pool,ownerid,size,objectid,lastmodifiedtime,etag," +
		"contenttype,customattributes,acl,nullversion,deletemarker,ssetype,encryptionkey,initializationvector,type,storageclass,createtime) " +
		"values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	args := []interface{}{o.BucketName, o.Name, o.VersionId, o.Location, o.Pool, o.OwnerId, o.Size, o.ObjectId,
		lastModifiedTime, o.Etag, o.ContentType, customAttributes, acl, o.NullVersion, o.DeleteMarker,
		o.SseType, o.EncryptionKey, o.InitializationVector, o.Type, o.StorageClass, o.LastModifiedTime.UnixNano()}
	return sql, args
}

func (o *Object) GetUpdateSql() (string, []interface{}) {
	customAttributes, _ := json.Marshal(o.CustomAttributes)
	acl, _ := json.Marshal(o.ACL)
	lastModifiedTime := o.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := "update objects set location=?,pool=?,size=?,objectid=?,lastmodifiedtime=?,etag=?,deletemarker=?," +
		"contenttype=?,customattributes=?,acl=?,ssetype=?,encryptionkey=?,initializationvector=?,type=?, storageclass=?, createtime=? " +
		"where bucketname=? and name=? and version=?"
	args := []interface{}{o.Location, o.Pool, o.Size, o.ObjectId,
		lastModifiedTime, o.Etag, o.DeleteMarker, o.ContentType, customAttributes, acl,
		o.SseType, o.EncryptionKey, o.InitializationVector, o.Type, o.StorageClass, o.LastModifiedTime.UnixNano(), o.BucketName, o.Name, o.VersionId}
	return sql, args
}

func (o *Object) GetUpdateHotSql() (string, []interface{}) {
	customAttributes, _ := json.Marshal(o.CustomAttributes)
	acl, _ := json.Marshal(o.ACL)
	lastModifiedTime := o.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := "update hotobjects set location=?,pool=?,size=?,objectid=?,lastmodifiedtime=?,etag=?,deletemarker=?," +
		"contenttype=?,customattributes=?,acl=?,ssetype=?,encryptionkey=?,initializationvector=?,type=?, storageclass=?, createtime=? " +
		"where bucketname=? and name=? and version=?"
	args := []interface{}{o.Location, o.Pool, o.Size, o.ObjectId,
		lastModifiedTime, o.Etag, o.DeleteMarker, o.ContentType, customAttributes, acl,
		o.SseType, o.EncryptionKey, o.InitializationVector, o.Type, o.StorageClass, o.LastModifiedTime.UnixNano(), o.BucketName, o.Name, o.VersionId}
	return sql, args
}

func (o *Object) GetRemoveHotSql() (string, []interface{}) {
	sql := "delete from hotobjects " +
		"where bucketname=? and name=? and version=?"
	args := []interface{}{o.BucketName, o.Name, o.VersionId}
	return sql, args
}

func (o *Object) GetUpdateAclSql() (string, []interface{}) {
	acl, _ := json.Marshal(o.ACL)
	sql := "update objects set acl=? where bucketname=? and name=? and version=?"
	args := []interface{}{acl, o.BucketName, o.Name, o.VersionId}
	return sql, args
}

func (o *Object) GetUpdateAttrsSql() (string, []interface{}) {
	customAttributes, _ := json.Marshal(o.CustomAttributes)
	sql := "update objects set customattributes=? where bucketname=? and name=?"
	args := []interface{}{customAttributes, o.BucketName, o.Name}
	return sql, args
}

func (o *Object) GetUpdateNameSql(sourceObject string) (string, []interface{}) {
	sql := "update objects set name=? where bucketname=? and name=? and version=0"
	args := []interface{}{o.Name, o.BucketName, sourceObject}
	return sql, args
}

func (o *Object) GetReplaceObjectMetasSql() (string, []interface{}) {
	customAttributes, _ := json.Marshal(o.CustomAttributes)
	sql := "update objects set contenttype=?,customattributes=?,storageclass=? where bucketname=? and name=? and version=?"
	args := []interface{}{o.ContentType, customAttributes, o.StorageClass, o.BucketName, o.Name, o.VersionId}
	return sql, args
}

func (o *Object) GetGlacierUpdateSql() (string, []interface{}) {
	sql := "update objects set location=?,pool=?," +
		"size=?,objectid=?,etag=?,initializationvector=?,storageclass=? where bucketname=? and name=? and version=?"
	args := []interface{}{o.Location, o.Pool, o.Size, o.ObjectId, o.Etag, o.InitializationVector, o.StorageClass, o.BucketName, o.Name, o.VersionId}
	return sql, args
}
