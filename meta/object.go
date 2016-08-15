package meta

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	. "git.letv.cn/yig/yig/error"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
	"math"
	"strconv"
	"strings"
	"time"
)

type Object struct {
	Rowkey           string // Rowkey cache
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
	Parts            map[int]Part
}

// Rowkey format:
// BucketName +
// bigEndian(uint16(count("/", ObjectName))) +
// ObjectName +
// bigEndian(uint64.max - unixNanoTimestamp)
func (o Object) GetRowkey() (string, error) {
	if o.Rowkey != "" {
		return o.Rowkey, nil
	}
	var rowkey bytes.Buffer
	rowkey.WriteString(o.BucketName)
	err := binary.Write(&rowkey, binary.BigEndian, uint16(strings.Count(o.Name, "/")))
	if err != nil {
		return "", err
	}
	rowkey.WriteString(o.Name)
	err = binary.Write(&rowkey, binary.BigEndian,
		math.MaxUint64-uint64(o.LastModifiedTime.UnixNano()))
	if err != nil {
		return "", err
	}
	o.Rowkey = rowkey.String()
	return o.Rowkey, nil
}

func (o Object) GetValues() (values map[string]map[string][]byte, err error) {
	var size bytes.Buffer
	err = binary.Write(&size, binary.BigEndian, o.Size)
	if err != nil {
		return
	}
	values = map[string]map[string][]byte{
		OBJECT_COLUMN_FAMILY: map[string][]byte{
			"location":     []byte(o.Location),
			"pool":         []byte(o.Pool),
			"owner":        []byte(o.OwnerId),
			"oid":          []byte(o.ObjectId),
			"size":         size.Bytes(),
			"lastModified": []byte(o.LastModifiedTime.Format(CREATE_TIME_LAYOUT)),
			"etag":         []byte(o.Etag),
			"content-type": []byte(o.ContentType),
			"attributes":   []byte{}, // TODO
		},
	}
	for partNumber, part := range o.Parts {
		var marshaled []byte
		marshaled, err = json.Marshal(part)
		if err != nil {
			return
		}
		if _, ok := values[OBJECT_PART_COLUMN_FAMILY]; !ok {
			values[OBJECT_PART_COLUMN_FAMILY] = make(map[string][]byte)
		}
		values[OBJECT_PART_COLUMN_FAMILY][strconv.Itoa(partNumber)] = marshaled
	}
	return
}

func (o Object) GetValuesForDelete() (values map[string]map[string][]byte) {
	return map[string]map[string][]byte{
		OBJECT_COLUMN_FAMILY: map[string][]byte{},
		OBJECT_PART_COLUMN_FAMILY: map[string][]byte{},
	}
}

// Rowkey format:
// BucketName +
// bigEndian(uint16(count("/", ObjectName))) +
// ObjectName +
// bigEndian(uint64.max - unixNanoTimestamp)
// The prefix excludes timestamp part
func getObjectRowkeyPrefix(bucketName string, objectName string) ([]byte, error) {
	var rowkey bytes.Buffer
	rowkey.WriteString(bucketName)
	err := binary.Write(&rowkey, binary.BigEndian, uint16(strings.Count(objectName, "/")))
	if err != nil {
		return []byte{}, err
	}
	rowkey.WriteString(objectName)
	return rowkey.Bytes(), nil
}

// Rowkey format:
// bigEndian(unixNanoTimestamp) + BucketName + ObjectName
func GetGarbageCollectionRowkey(bucketName string, objectName string) (string, error) {
	var rowkey bytes.Buffer
	err := binary.Write(&rowkey, binary.BigEndian,
		uint64(time.Now().UnixNano()))
	if err != nil {
		return "", err
	}
	rowkey.WriteString(bucketName)
	rowkey.WriteString(objectName)
	return rowkey.String(), nil
}

// Decode response from HBase and return an Object object
func ObjectFromResponse(response *hrpc.Result, bucketName string) (object Object, err error) {
	var rowkey []byte
	object.Parts = make(map[int]Part)
	for _, cell := range response.Cells {
		rowkey = cell.Row
		switch string(cell.Family) {
		case OBJECT_COLUMN_FAMILY:
			switch string(cell.Qualifier) {
			case "location":
				object.Location = string(cell.Value)
			case "pool":
				object.Pool = string(cell.Value)
			case "owner":
				object.OwnerId = string(cell.Value)
			case "size":
				err = binary.Read(bytes.NewReader(cell.Value), binary.BigEndian,
					&object.Size)
				if err != nil {
					return
				}
			case "oid":
				object.ObjectId = string(cell.Value)
			case "lastModified":
				object.LastModifiedTime, err = time.Parse(CREATE_TIME_LAYOUT,
					string(cell.Value))
				if err != nil {
					return
				}
			case "etag":
				object.Etag = string(cell.Value)
			case "content-type":
				object.ContentType = string(cell.Value)
			}
		case OBJECT_PART_COLUMN_FAMILY:
			var partNumber int
			partNumber, err = strconv.Atoi(string(cell.Qualifier))
			if err != nil {
				return
			}
			var p Part
			err = json.Unmarshal(cell.Value, &p)
			if err != nil {
				return
			}
			object.Parts[partNumber] = p
		}
	}
	object.BucketName = bucketName
	object.Rowkey = string(rowkey)
	// rowkey = BucketName + bigEndian(uint16(count("/", ObjectName)))
	// + ObjectName
	// + bigEndian(uint64.max - unixNanoTimestamp)
	object.Name = string(rowkey[len(bucketName)+2 : len(rowkey)-8])
	return
}

func (m *Meta) GetObject(bucketName string, objectName string) (object Object, err error) {
	objectRowkeyPrefix, err := getObjectRowkeyPrefix(bucketName, objectName)
	if err != nil {
		return
	}
	filter := filter.NewPrefixFilter(objectRowkeyPrefix)
	scanRequest, err := hrpc.NewScanRangeStr(context.Background(), OBJECT_TABLE,
		string(objectRowkeyPrefix), "", hrpc.Filters(filter), hrpc.NumberOfRows(1))
	if err != nil {
		return
	}
	scanResponse, err := m.Hbase.Scan(scanRequest)
	if err != nil {
		return
	}
	if len(scanResponse) == 0 {
		err = ErrNoSuchKey
		return
	}
	object, err = ObjectFromResponse(scanResponse[0], bucketName)
	if err != nil {
		return
	}
	if object.Name != objectName {
		err = ErrNoSuchKey
		return
	}
	return
}
