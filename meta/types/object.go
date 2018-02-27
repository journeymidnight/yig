package types

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/util"
	"github.com/xxtea/xxtea-go/xxtea"
	"io"
	"math"
	"strconv"
	"time"
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
	// type of Server Side Encryption, could be "KMS", "S3", "C"(custom), or ""(none),
	// KMS is not implemented yet
	SseType string
	// encryption key for SSE-S3, the key itself is encrypted with SSE_S3_MASTER_KEY,
	// in AES256-GCM
	EncryptionKey        []byte
	InitializationVector []byte
}

func (o *Object) String() (s string) {
	s += "Name: " + o.Name + "\n"
	s += "Location: " + o.Location + "\n"
	s += "Pool: " + o.Pool + "\n"
	s += "Object ID: " + o.ObjectId + "\n"
	s += "Last Modified Time: " + o.LastModifiedTime.Format(CREATE_TIME_LAYOUT) + "\n"
	s += "Version: " + o.VersionId + "\n"
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

// Rowkey format:
// BucketName +
// bigEndian(uint16(count("/", ObjectName))) +
// ObjectName +
// ObjectNameEnding +
// bigEndian(uint64.max - unixNanoTimestamp)
func (o *Object) GetRowkey() (string, error) {
	if len(o.Rowkey) != 0 {
		return string(o.Rowkey), nil
	}
	var rowkey bytes.Buffer
	rowkey.WriteString(o.BucketName + ObjectNameSeparator)
	rowkey.WriteString(o.Name + ObjectNameSeparator)
	err := binary.Write(&rowkey, binary.BigEndian,
		math.MaxUint64-uint64(o.LastModifiedTime.UnixNano()))
	if err != nil {
		return "", err
	}
	o.Rowkey = rowkey.Bytes()
	return string(o.Rowkey), nil
}

func (o *Object) GetValues() (values map[string]map[string][]byte, err error) {
	var size bytes.Buffer
	err = binary.Write(&size, binary.BigEndian, o.Size)
	if err != nil {
		return
	}
	err = o.encryptSseKey()
	if err != nil {
		return
	}
	if o.EncryptionKey == nil {
		o.EncryptionKey = []byte{}
	}
	if o.InitializationVector == nil {
		o.InitializationVector = []byte{}
	}
	var attrsData []byte
	if o.CustomAttributes != nil {
		attrsData, err = json.Marshal(o.CustomAttributes)
		if err != nil {
			return
		}
	}
	values = map[string]map[string][]byte{
		OBJECT_COLUMN_FAMILY: map[string][]byte{
			"bucket":        []byte(o.BucketName),
			"location":      []byte(o.Location),
			"pool":          []byte(o.Pool),
			"owner":         []byte(o.OwnerId),
			"oid":           []byte(o.ObjectId),
			"size":          size.Bytes(),
			"lastModified":  []byte(o.LastModifiedTime.Format(CREATE_TIME_LAYOUT)),
			"etag":          []byte(o.Etag),
			"content-type":  []byte(o.ContentType),
			"attributes":    attrsData, // TODO
			"ACL":           []byte(o.ACL.CannedAcl),
			"nullVersion":   []byte(helper.Ternary(o.NullVersion, "true", "false").(string)),
			"deleteMarker":  []byte(helper.Ternary(o.DeleteMarker, "true", "false").(string)),
			"sseType":       []byte(o.SseType),
			"encryptionKey": o.EncryptionKey,
			"IV":            o.InitializationVector,
		},
	}
	if len(o.Parts) != 0 {
		values[OBJECT_PART_COLUMN_FAMILY], err = valuesForParts(o.Parts)
		if err != nil {
			return
		}
	}
	return
}

func (o *Object) GetValuesForDelete() (values map[string]map[string][]byte) {
	return map[string]map[string][]byte{
		OBJECT_COLUMN_FAMILY:      map[string][]byte{},
		OBJECT_PART_COLUMN_FAMILY: map[string][]byte{},
	}
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

func (o *Object) GetCreateSql() string {
	version := math.MaxUint64 - uint64(o.LastModifiedTime.UnixNano())
	customAttributes, _ := json.Marshal(o.CustomAttributes)
	acl, _ := json.Marshal(o.ACL)
	lastModifiedTime := o.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := fmt.Sprintf("insert into objects values('%s','%s',%d,'%s','%s','%s','%d','%s','%s','%s','%s','%s','%s',%t,%t,'%s','%s','%s')", o.BucketName, o.Name, version, o.Location, o.Pool, o.OwnerId, o.Size, o.ObjectId, lastModifiedTime, o.Etag, o.ContentType, customAttributes, acl, o.NullVersion, o.DeleteMarker, o.SseType, o.EncryptionKey, o.InitializationVector)
	fmt.Println(sql)
	fmt.Println("exit getcreateslql")
	return sql
}
