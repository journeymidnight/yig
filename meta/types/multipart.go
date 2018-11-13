package types

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/xxtea/xxtea-go/xxtea"
	"math"
	"strconv"
	"strings"
	"time"
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

// For scenario only one part is needed to insert
func (p *Part) GetValues() (values map[string]map[string][]byte, err error) {
	marshaledPart, err := json.Marshal(p)
	if err != nil {
		return
	}
	values = map[string]map[string][]byte{
		MULTIPART_COLUMN_FAMILY: map[string][]byte{
			strconv.Itoa(p.PartNumber): marshaledPart,
		},
	}
	return
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
}

type Multipart struct {
	BucketName  string
	ObjectName  string
	InitialTime time.Time
	UploadId    string // upload id cache
	Metadata    MultipartMetadata
	Parts       map[int]*Part
}

// Multipart table rowkey format:
// BucketName +
// bigEndian(uint16(count("/", ObjectName))) +
// ObjectName +
// bigEndian(unixNanoTimestamp)
func (m *Multipart) GetRowkey() (string, error) {
	var rowkey bytes.Buffer
	rowkey.WriteString(m.BucketName)
	err := binary.Write(&rowkey, binary.BigEndian, uint16(strings.Count(m.ObjectName, "/")))
	if err != nil {
		return "", err
	}
	rowkey.WriteString(m.ObjectName)
	err = binary.Write(&rowkey, binary.BigEndian, uint64(m.InitialTime.UnixNano()))
	if err != nil {
		return "", err
	}
	return rowkey.String(), nil
}

func (m *Multipart) GetValues() (values map[string]map[string][]byte, err error) {
	values = make(map[string]map[string][]byte)

	values[MULTIPART_COLUMN_FAMILY], err = valuesForParts(m.Parts)
	if err != nil {
		return
	}

	var marshaledMeta []byte
	marshaledMeta, err = json.Marshal(m.Metadata)
	if err != nil {
		return
	}
	if values[MULTIPART_COLUMN_FAMILY] == nil {
		values[MULTIPART_COLUMN_FAMILY] = make(map[string][]byte)
	}
	values[MULTIPART_COLUMN_FAMILY]["0"] = marshaledMeta
	return
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

func (m *Multipart) GetValuesForDelete() map[string]map[string][]byte {
	return map[string]map[string][]byte{
		MULTIPART_COLUMN_FAMILY: map[string][]byte{},
	}
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

func (p *Part) GetCreateSql(bucketname, objectname, version string) string {
	sql := fmt.Sprintf("insert into objectpart(partnumber,size,objectid,offset,etag,lastmodified,initializationvector,bucketname,objectname,version) "+
		"values(%d,%d,'%s',%d,'%s','%s','%s','%s','%s','%s')",
		p.PartNumber, p.Size, p.ObjectId, p.Offset, p.Etag, p.LastModified, p.InitializationVector, bucketname, objectname, version)
	return sql
}

func (p *Part) GetCreateGcSql(bucketname, objectname string, version uint64) string {
	sql := fmt.Sprintf("insert into gcpart(partnumber,size,objectid,offset,etag,lastmodified,initializationvector,bucketname,objectname,version) "+
		"values(%d,%d,'%s',%d,'%s','%s','%s','%s','%s',%d)",
		p.PartNumber, p.Size, p.ObjectId, p.Offset, p.Etag, p.LastModified, p.InitializationVector, bucketname, objectname, version)
	return sql
}
