package meta

import (
	"bytes"
	"encoding/binary"
	"github.com/tsuna/gohbase"
	"log"
	"math"
	"strings"
	"time"
)

const (
	ZOOKEEPER_ADDRESS = "10.116.77.35:2181,10.116.77.36:2181,10.116.77.37:2181"
	RETRY_LIMIT       = 3

	BUCKET_TABLE         = "buckets"
	BUCKET_COLUMN_FAMILY = "b"
	USER_TABLE           = "users"
	USER_COLUMN_FAMILY   = "u"
	OBJECT_TABLE         = "objects"
	OBJECT_COLUMN_FAMILY = "o"

	CREATE_TIME_LAYOUT = "2006-01-02T15:04:05.000Z"
)

type Meta struct {
	Hbase  gohbase.Client
	Logger *log.Logger
	// TODO Redis and more
}

func New(logger *log.Logger) *Meta {
	hbase := gohbase.NewClient(ZOOKEEPER_ADDRESS)
	meta := Meta{
		Hbase:  hbase,
		Logger: logger,
	}
	return &meta
}

type Bucket struct {
	Name string
	// Date and time when the bucket was created,
	// in format "2006-01-02T15:04:05.000Z"
	CreateTime string
	OwnerId    string
	CORS       string
	ACL        string
}

type Object struct {
	Name             string
	BucketName       string
	Location         string // which Ceph cluster this object locates
	Pool             string // which Ceph pool this object locates
	OwnerId          string
	Size             uint64    // file size
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
	return rowkey.String(), nil
}

func (o Object) GetValues() (values map[string]map[string][]byte, err error) {
	var size bytes.Buffer
	err = binary.Write(size, binary.BigEndian, o.Size)
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
