package types

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/meta/util"
	"github.com/xxtea/xxtea-go/xxtea"
)

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
}

type ObjectType int

const (
	ObjectTypeNormal ObjectType = iota
	ObjectTypeAppendable
	ObjectTypeMultipart
)

func (o *Object) ObjectTypeToString() string {
	switch o.Type {
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
	s += "Name: " + o.Name + "\n"
	s += "Location: " + o.Location + "\n"
	s += "Pool: " + o.Pool + "\n"
	s += "Object ID: " + o.ObjectId + "\n"
	s += "Last Modified Time: " + o.LastModifiedTime.Format(CREATE_TIME_LAYOUT) + "\n"
	s += "Version: " + o.VersionId + "\n"
	s += "Type: " + o.ObjectTypeToString() + "\n"
	s += "StorageClass: " + o.StorageClass.ToString() + "\n"
	for n, part := range o.Parts {
		s += fmt.Sprintln("Part", n, "Object ID:", part.ObjectId)
	}
	return s
}

func (o *Object) GetVersionNumber() (uint64, error) {
	decrypted, err := util.Decrypt(o.VersionId)
	if err != nil {
		return 0, err
	}
	version, err := strconv.ParseUint(decrypted, 10, 64)
	if err != nil {
		return 0, err
	}
	return version, nil
}

func (o *Object) encryptSseKey() (err error) {
	// Don't encrypt if `EncryptionKey` is not set
	if len(o.EncryptionKey) == 0 {
		return
	}

	if len(o.InitializationVector) == 0 {
		o.InitializationVector = make([]byte, INITIALIZATION_VECTOR_LENGTH)
		_, err = io.ReadFull(rand.Reader, o.InitializationVector)
		if err != nil {
			return
		}
	}

	block, err := aes.NewCipher(SSE_S3_MASTER_KEY)
	if err != nil {
		return err
	}

	aesGcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}

	// InitializationVector is 16 bytes(because of CTR), but use only first 12 bytes in GCM
	// for performance
	o.EncryptionKey = aesGcm.Seal(nil, o.InitializationVector[:12], o.EncryptionKey, nil)
	return nil
}

func (o *Object) GetVersionId() string {
	if o.NullVersion {
		return "null"
	}
	if o.VersionId != "" {
		return o.VersionId
	}
	timeData := []byte(strconv.FormatUint(uint64(o.LastModifiedTime.UnixNano()), 10))
	o.VersionId = hex.EncodeToString(xxtea.Encrypt(timeData, XXTEA_KEY))
	return o.VersionId
}

//Tidb related function

func (o *Object) GetCreateSql() (string, []interface{}) {
	version := math.MaxUint64 - uint64(o.LastModifiedTime.UnixNano())
	customAttributes, _ := json.Marshal(o.CustomAttributes)
	acl, _ := json.Marshal(o.ACL)
	lastModifiedTime := o.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := "insert into objects values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	args := []interface{}{o.BucketName, o.Name, version, o.Location, o.Pool, o.OwnerId, o.Size, o.ObjectId,
		lastModifiedTime, o.Etag, o.ContentType, customAttributes, acl, o.NullVersion, o.DeleteMarker,
		o.SseType, o.EncryptionKey, o.InitializationVector, o.Type, o.StorageClass}
	return sql, args
}

func (o *Object) GetAppendSql() (string, []interface{}) {
	version := math.MaxUint64 - uint64(o.LastModifiedTime.UnixNano())
	lastModifiedTime := o.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := "update objects set lastmodifiedtime=?, size=?, version=? where bucketname=? and name=?"
	args := []interface{}{lastModifiedTime, o.Size, version, o.BucketName, o.Name}
	return sql, args
}

func (o *Object) GetUpdateAclSql() (string, []interface{}) {
	version := math.MaxUint64 - uint64(o.LastModifiedTime.UnixNano())
	acl, _ := json.Marshal(o.ACL)
	sql := "update objects set acl=? where bucketname=? and name=? and version=?"
	args := []interface{}{acl, o.BucketName, o.Name, version}
	return sql, args
}

func (o *Object) GetUpdateAttrsSql() (string, []interface{}) {
	version := math.MaxUint64 - uint64(o.LastModifiedTime.UnixNano())
	attrs, _ := json.Marshal(o.CustomAttributes)
	sql := "update objects set customattributes =?, storageclass=? where bucketname=? and name=? and version=?"
	args := []interface{}{attrs, o.StorageClass, o.BucketName, o.Name, version}
	return sql, args

}

func (o *Object) GetAddUsageSql() (string, []interface{}) {
	sql := "update buckets set usages= usages + ? where bucketname=?"
	args := []interface{}{o.Size, o.BucketName}
	return sql, args
}

func (o *Object) GetSubUsageSql() (string, []interface{}) {
	sql := "update buckets set usages= usages + ? where bucketname=?"
	args := []interface{}{-o.Size, o.BucketName}
	return sql, args
}
