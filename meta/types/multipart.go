package types

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/meta/util"
)

type Part struct {
	PartNumber int
	Size       int64
	ObjectId   string

	// offset of this part in whole object, calculated when moving parts from
	// `multiparts` table to `objects` table
	Offset               int64
	Etag                 string
	LastModified         string // time string of format "2006-01-02T15:04:05.000Z"
	InitializationVector []byte
}

type MultipartMetadata struct {
	InitiatorId   string
	OwnerId       string
	ContentType   string
	Location      string
	Pool          string
	Acl           datatype.Acl
	SseRequest    datatype.SseRequest
	EncryptionKey []byte
	CipherKey     []byte
	Attrs         map[string]string
	StorageClass  StorageClass
}

type Multipart struct {
	BucketName  string
	ObjectName  string
	InitialTime uint64
	UploadId    string // upload id cache
	Metadata    MultipartMetadata
	Parts       map[int]*Part
}

func (m *Multipart) GenUploadId() error {
	if m.UploadId != "" {
		return nil
	}
	if m.InitialTime == 0 {
		return errors.New("Zero value InitialTime for Multipart")
	}

	m.UploadId = getMultipartUploadId(m.InitialTime)
	return nil
}

//UploadId = xxtea.Encrypt(BigEndian(MaxUint64 - UTC.Nano()), "hehehehe")
func EncodeTime(initialTime uint64) []byte {
	var bin [8]byte
	binary.BigEndian.PutUint64(bin[:], math.MaxUint64-initialTime)
	return bin[:]
}

//UploadId = xxtea.Encrypt(BigEndian(MaxUint64 - UTC.Nano()), "hehehehe")
func DecodeTime(bin []byte) uint64 {
	t := binary.BigEndian.Uint64(bin)
	return math.MaxUint64 - t
}

func getMultipartUploadId(initialTime uint64) string {
	return util.Encrypt(EncodeTime(initialTime))
}

func GetInitialTimeFromUploadId(uploadId string) (uint64, error) {
	bin, err := util.Decrypt(uploadId)
	if err != nil {
		return 0, err
	}
	return DecodeTime(bin), nil
}

func GetMultipartUploadIdByDbTime(uploadtime uint64) string {
	initialTime := math.MaxUint64 - uploadtime
	return getMultipartUploadId(initialTime)
}

func (p *Part) GetCreateSql(bucketname, objectname, version string) (string, []interface{}) {
	sql := "insert into objectpart(partnumber,size,objectid,offset,etag,lastmodified,initializationvector,bucketname,objectname,version) " +
		"values(?,?,?,?,?,?,?,?,?,?)"
	args := []interface{}{p.PartNumber, p.Size, p.ObjectId, p.Offset, p.Etag, p.LastModified, p.InitializationVector, bucketname, objectname, version}
	return sql, args
}

func (p *Part) GetCreateGcSql(bucketname, objectname string, version uint64) (string, []interface{}) {
	sql := "insert into gcpart(partnumber,size,objectid,offset,etag,lastmodified,initializationvector,bucketname,objectname,version) " +
		"values(?,?,?,?,?,?,?,?,?,?)"
	args := []interface{}{p.PartNumber, p.Size, p.ObjectId, p.Offset, p.Etag, p.LastModified, p.InitializationVector, bucketname, objectname, version}
	return sql, args
}

func (o *Object) GetUpdateObjectPartNameSql(sourceObject string) (string, []interface{}) {
	version := math.MaxUint64 - o.CreateTime
	sql := "update objectpart set objectname=? where bucketname=? and objectname=? and version=?"
	args := []interface{}{o.Name, o.BucketName, sourceObject, version}
	return sql, args
}
