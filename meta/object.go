package meta

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/helper"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/xxtea/xxtea-go/xxtea"
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
	ACL              datatype.Acl
	NullVersion      bool   // if this entry has `null` version
	DeleteMarker     bool   // if this entry is a delete marker
	VersionId        string // version cache
}

func (o Object) String() (s string) {
	s += "Name: " + o.Name + "\n"
	s += "Location: " + o.Location + "\n"
	s += "Pool: " + o.Pool + "\n"
	s += "Object ID: " + o.ObjectId + "\n"
	for n, part := range o.Parts {
		s += fmt.Sprintln("Part", n, " Location:", part.Location, "Pool:", part.Pool,
			"Object ID:", part.ObjectId)
	}
	return s
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
			"ACL":          []byte(o.ACL.CannedAcl),
			"version":      []byte(helper.Ternary(o.NullVersion, "true", "false").(string)),
			"deleteMarker": []byte(helper.Ternary(o.DeleteMarker, "true", "false").(string)),
		},
	}
	if len(o.Parts) != 0 {
		values[OBJECT_PART_COLUMN_FAMILY], err = ValuesForParts(o.Parts)
		if err != nil {
			return
		}
	}
	return
}

func (o Object) GetValuesForDelete() (values map[string]map[string][]byte) {
	return map[string]map[string][]byte{
		OBJECT_COLUMN_FAMILY:      map[string][]byte{},
		OBJECT_PART_COLUMN_FAMILY: map[string][]byte{},
	}
}

func (o Object) GetVersionId() string {
	if o.VersionId != "" {
		return o.VersionId
	}
	if o.NullVersion {
		o.VersionId = "null"
		return o.VersionId
	}
	timeData := []byte(strconv.FormatUint(uint64(o.LastModifiedTime.UnixNano()), 10))
	o.VersionId = hex.EncodeToString(xxtea.Encrypt(timeData, XXTEA_KEY))
	return o.VersionId
}

// Rowkey format:
// BucketName +
// bigEndian(uint16(count("/", ObjectName))) +
// ObjectName +
// bigEndian(uint64.max - unixNanoTimestamp)
// The prefix excludes timestamp part if version is empty
func getObjectRowkeyPrefix(bucketName string, objectName string, version string) ([]byte, error) {
	var rowkey bytes.Buffer
	rowkey.WriteString(bucketName)
	err := binary.Write(&rowkey, binary.BigEndian, uint16(strings.Count(objectName, "/")))
	if err != nil {
		return []byte{}, err
	}
	rowkey.WriteString(objectName)
	if version != "" {
		versionBytes, err := hex.DecodeString(version)
		if err != nil {
			return []byte{}, err
		}
		decrypted := xxtea.Decrypt(versionBytes, XXTEA_KEY)
		unixNanoTimestamp, errno := binary.Uvarint(decrypted)
		if errno <= 0 {
			return []byte{}, ErrInvalidVersioning
		}
		err = binary.Write(&rowkey, binary.BigEndian,
			math.MaxUint64-unixNanoTimestamp)
		if err != nil {
			return []byte{}, err
		}
	}
	return rowkey.Bytes(), nil
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
			case "ACL":
				object.ACL.CannedAcl = string(cell.Value)
			case "version":
				object.NullVersion = helper.Ternary(string(cell.Value) == "true",
					true, false).(bool)
			case "deleteMarker":
				object.DeleteMarker = helper.Ternary(string(cell.Value) == "true",
					true, false).(bool)
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
	if object.NullVersion {
		object.VersionId = "null"
	} else {
		reversedTimeBytes := rowkey[len(rowkey)-8:]
		var reversedTime uint64
		err = binary.Read(bytes.NewReader(reversedTimeBytes), binary.BigEndian,
			&reversedTime)
		if err != nil {
			return
		}
		timestamp := math.MaxUint64 - reversedTime
		timeData := []byte(strconv.FormatUint(timestamp, 10))
		object.VersionId = hex.EncodeToString(xxtea.Encrypt(timeData, XXTEA_KEY))
	}
	return
}

func (m *Meta) GetObject(bucketName string, objectName string) (object Object, err error) {
	objectRowkeyPrefix, err := getObjectRowkeyPrefix(bucketName, objectName, "")
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

func (m *Meta) GetNullVersionObject(bucketName, objectName string) (object Object, err error) {
	objectRowkeyPrefix, err := getObjectRowkeyPrefix(bucketName, objectName, "")
	if err != nil {
		return
	}
	filter := filter.NewPrefixFilter(objectRowkeyPrefix)
	// FIXME use a proper filter instead of naively getting 1000 and compare
	scanRequest, err := hrpc.NewScanRangeStr(context.Background(), OBJECT_TABLE,
		string(objectRowkeyPrefix), "", hrpc.Filters(filter), hrpc.NumberOfRows(1000))
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
	for _, response := range scanResponse {
		object, err = ObjectFromResponse(response, bucketName)
		if err != nil {
			return
		}
		if object.Name == objectName && object.NullVersion {
			return object, nil
		}
	}
	return object, ErrNoSuchKey
}

func (m *Meta) GetObjectVersion(bucketName, objectName, version string) (object Object, err error) {
	objectRowkeyPrefix, err := getObjectRowkeyPrefix(bucketName, objectName, version)
	if err != nil {
		return
	}
	getRequest, err := hrpc.NewGetStr(context.Background(), OBJECT_TABLE, string(objectRowkeyPrefix))
	if err != nil {
		return
	}
	getResponse, err := m.Hbase.Get(getRequest)
	if err != nil {
		return
	}
	if len(getResponse.Cells) == 0 {
		err = ErrNoSuchVersion
		return
	}
	object, err = ObjectFromResponse(getResponse, bucketName)
	if err != nil {
		return
	}
	if object.Name != objectName {
		err = ErrNoSuchKey
		return
	}
	return
}
