package types

import (
	"errors"
	"math"
	"strconv"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/common"
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
	StorageClass  common.StorageClass
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

// UploadId := hex.EncodeToString(xxtea.Encrypt(TIME_STRING, XXTEA_KEY))
func getMultipartUploadId(initialTime uint64) string {
	timeData := strconv.FormatUint(initialTime, 10)
	helper.Logger.Info("timeData:")
	return common.Encrypt(timeData)
}

func GetInitialTimeFromUploadId(uploadId string) (uint64, error) {
	timeStr, err := common.Decrypt(uploadId)
	if err != nil {
		return 0, err
	}
	initialTime, err := strconv.ParseUint(timeStr, 10, 64)
	if err != nil {
		return 0, err
	}
	return initialTime, nil
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
