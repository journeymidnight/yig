package types

import (
	"fmt"
	"github.com/journeymidnight/yig-restore/meta/common"
	"github.com/journeymidnight/yig-restore/restore/datatype"
	"math"
	"strconv"
	"time"
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
	StorageClass common.StorageClass
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

	return strconv.FormatUint(math.MaxUint64-uint64(o.LastModifiedTime.UnixNano()), 10)
}
