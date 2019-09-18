package types

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/xxtea/xxtea-go/xxtea"
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
	InitialTime time.Time
	UploadId    string // upload id cache
	Metadata    MultipartMetadata
	Parts       map[int]*Part
}


func (m *Multipart) GetUploadId() (string, error) {
	if m.UploadId != "" {
		return m.UploadId, nil
	}
	if m.InitialTime.IsZero() {
		return "", errors.New("Zero value InitialTime for Multipart")
	}
	m.UploadId = getMultipartUploadId(m.InitialTime)
	return m.UploadId, nil
}
func getMultipartUploadId(t time.Time) string {
	timeData := []byte(strconv.FormatUint(uint64(t.UnixNano()), 10))
	return hex.EncodeToString(xxtea.Encrypt(timeData, XXTEA_KEY))
}

func GetMultipartUploadIdForTidb(uploadtime uint64) string {
	realUploadTime := math.MaxUint64 - uploadtime
	timeData := []byte(strconv.FormatUint(realUploadTime, 10))
	return hex.EncodeToString(xxtea.Encrypt(timeData, XXTEA_KEY))
}

func valuesForParts(parts map[int]*Part) (values map[string][]byte, err error) {
	for partNumber, part := range parts {
		var marshaled []byte
		marshaled, err = json.Marshal(part)
		if err != nil {
			return
		}
		if values == nil {
			values = make(map[string][]byte)
		}
		values[strconv.Itoa(partNumber)] = marshaled
	}
	return
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
