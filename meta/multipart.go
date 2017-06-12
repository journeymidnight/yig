package meta

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/cannium/gohbase/hrpc"
	"github.com/xxtea/xxtea-go/xxtea"
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

func (m *Multipart) GetValuesForDelete() map[string]map[string][]byte {
	return map[string]map[string][]byte{
		MULTIPART_COLUMN_FAMILY: map[string][]byte{},
	}
}

func MultipartFromResponse(response *hrpc.Result, bucketName string) (multipart Multipart,
	err error) {

	var rowkey []byte
	multipart.Parts = make(map[int]*Part)
	for _, cell := range response.Cells {
		rowkey = cell.Row
		var partNumber int
		partNumber, err = strconv.Atoi(string(cell.Qualifier))
		if err != nil {
			return
		}
		if partNumber == 0 {
			err = json.Unmarshal(cell.Value, &multipart.Metadata)
			if err != nil {
				return
			}
		} else {
			var p Part
			err = json.Unmarshal(cell.Value, &p)
			if err != nil {
				return
			}
			multipart.Parts[partNumber] = &p
		}
	}
	multipart.BucketName = bucketName
	multipart.ObjectName = string(rowkey[len(bucketName)+2 : len(rowkey)-8])

	timeBytes := rowkey[len(rowkey)-8:]
	var timestamp uint64
	err = binary.Read(bytes.NewReader(timeBytes), binary.BigEndian, &timestamp)
	if err != nil {
		return
	}
	multipart.InitialTime = time.Unix(0, int64(timestamp))

	return
}

func (m *Meta) GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error) {
	rowkey, err := getMultipartRowkeyFromUploadId(bucketName, objectName, uploadId)
	if err != nil {
		helper.ErrorIf(err, "Unable to get multipart row key.")
		err = ErrNoSuchUpload
		return
	}
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	getMultipartRequest, err := hrpc.NewGetStr(ctx, MULTIPART_TABLE, rowkey)
	if err != nil {
		return
	}
	getMultipartResponse, err := m.Hbase.Get(getMultipartRequest)
	if err != nil {
		return
	}
	if len(getMultipartResponse.Cells) == 0 {
		err = ErrNoSuchUpload
		return
	}
	return MultipartFromResponse(getMultipartResponse, bucketName)
}

func getMultipartRowkeyFromUploadId(bucketName, objectName, uploadId string) (string, error) {
	var rowkey bytes.Buffer
	rowkey.WriteString(bucketName)
	err := binary.Write(&rowkey, binary.BigEndian, uint16(strings.Count(objectName, "/")))
	if err != nil {
		return "", err
	}
	rowkey.WriteString(objectName)
	timestampString, err := Decrypt(uploadId)
	if err != nil {
		return "", err
	}
	timestamp, err := strconv.ParseUint(timestampString, 10, 64)
	if err != nil {
		return "", err
	}
	err = binary.Write(&rowkey, binary.BigEndian, timestamp)
	if err != nil {
		return "", err
	}
	return rowkey.String(), nil
}

func getMultipartUploadId(t time.Time) string {
	timeData := []byte(strconv.FormatUint(uint64(t.UnixNano()), 10))
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
