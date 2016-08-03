package meta

import (
	"bytes"
	"encoding/binary"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
	"math"
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
}

// Rowkey format:
// BucketName +
// bigEndian(uint16(count("/", ObjectName))) +
// ObjectName +
// bigEndian(uint64.max - unixNanoTimestamp)
func (o Object) GetRowkey() (string, error) {
	if o.Rowkey != "" {
		return o.Rowkey
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
	return map[string]map[string][]byte{
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
	}, nil
}

func (o Object) GetValuesForDelete() (values map[string]map[string][]byte) {
	return map[string]map[string][]byte{
		OBJECT_COLUMN_FAMILY: map[string][]byte{},
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
		err = ObjectNotFound{
			Bucket: bucketName,
			Object: objectName,
		}
		return
	}
	for _, cell := range scanResponse[0].Cells {
		if !bytes.HasPrefix(cell.Row, objectRowkeyPrefix) {
			err = ObjectNotFound{
				Bucket: bucketName,
				Object: objectName,
			}
			return
		}
		object.Rowkey = string(cell.Row)
		switch string(cell.Qualifier) {
		case "lastModified":
			object.LastModifiedTime, err = time.Parse(CREATE_TIME_LAYOUT, string(cell.Value))
			if err != nil {
				return
			}
		case "size":
			err = binary.Read(bytes.NewReader(cell.Value), binary.BigEndian, &object.Size)
			if err != nil {
				return
			}
		case "content-type":
			object.ContentType = string(cell.Value)
		case "etag":
			object.Etag = string(cell.Value)
		case "oid":
			object.ObjectId = string(cell.Value)
		case "location":
			object.Location = string(cell.Value)
		case "pool":
			object.Pool = string(cell.Value)
		}
	}
	object.BucketName = bucketName
	object.Name = objectName
	return
}
