package meta

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/cannium/gohbase/filter"
	"github.com/cannium/gohbase/hrpc"
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/redis"
	"github.com/xxtea/xxtea-go/xxtea"
)

const (
	ObjectNameEnding      = ":"
	ObjectNameSeparator   = "\n"
	ObjectNameSmallestStr = " "
	ResponseNumberOfRows  = 1024
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

type ObjMap struct {
	Rowkey     []byte // Rowkey cache
	Name       string
	BucketName string
	NullVerNum uint64
	NullVerId  string
}

func (om *ObjMap) GetRowKey() (string, error) {
	if len(om.Rowkey) != 0 {
		return string(om.Rowkey), nil
	}
	var rowkey bytes.Buffer
	rowkey.WriteString(om.BucketName + ObjectNameSeparator)

	rowkey.WriteString(om.Name + ObjectNameSeparator)

	om.Rowkey = rowkey.Bytes()
	return string(om.Rowkey), nil
}

func (om *ObjMap) GetValues() (values map[string]map[string][]byte, err error) {
	var nullVerNum bytes.Buffer
	err = binary.Write(&nullVerNum, binary.BigEndian, om.NullVerNum)
	if err != nil {
		return
	}
	values = map[string]map[string][]byte{
		OBJMAP_COLUMN_FAMILY: map[string][]byte{
			"nullVerNum": nullVerNum.Bytes(),
		},
	}
	return
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
	decrypted, err := Decrypt(o.VersionId)
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

func (om *ObjMap) GetValuesForDelete() (values map[string]map[string][]byte) {
	return map[string]map[string][]byte{
		OBJMAP_COLUMN_FAMILY: map[string][]byte{},
	}
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

// Rowkey format:
// BucketName + ObjectNameSeparator + ObjectName + ObjectNameSeparator +
// bigEndian(uint64.max - unixNanoTimestamp)
// The prefix excludes timestamp part if version is empty
func getObjectRowkeyPrefix(bucketName string, objectName string, version string) ([]byte, error) {
	var rowkey bytes.Buffer
	rowkey.WriteString(bucketName + ObjectNameSeparator)
	rowkey.WriteString(objectName + ObjectNameSeparator)
	if version != "" {
		decrypted, err := Decrypt(version)
		if err != nil {
			return []byte{}, err
		}
		unixNanoTimestamp, err := strconv.ParseUint(decrypted, 10, 64)
		if err != nil {
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
func ObjectFromResponse(response *hrpc.Result) (object *Object, err error) {
	var rowkey []byte
	object = new(Object)
	object.Parts = make(map[int]*Part)
	for _, cell := range response.Cells {
		rowkey = cell.Row
		switch string(cell.Family) {
		case OBJECT_COLUMN_FAMILY:
			switch string(cell.Qualifier) {
			case "bucket":
				object.BucketName = string(cell.Value)
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
			case "nullVersion":
				object.NullVersion = helper.Ternary(string(cell.Value) == "true",
					true, false).(bool)
			case "deleteMarker":
				object.DeleteMarker = helper.Ternary(string(cell.Value) == "true",
					true, false).(bool)
			case "sseType":
				object.SseType = string(cell.Value)
			case "encryptionKey":
				object.EncryptionKey = cell.Value
			case "IV":
				object.InitializationVector = cell.Value
			case "attributes":
				if len(cell.Value) != 0 {
					var attrs map[string]string
					err = json.Unmarshal(cell.Value, &attrs)
					if err != nil {
						return
					}
					object.CustomAttributes = attrs
				}
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
			//		p.Etag = ""         // The member is not used, so give it null value
			//		p.LastModified = "" // The member is not used, so give it null value
			object.Parts[partNumber] = &p
		}
	}

	//build simple index for multipart
	if len(object.Parts) != 0 {
		var sortedPartNum = make([]int64, len(object.Parts))
		for k, v := range object.Parts {
			sortedPartNum[k-1] = v.Offset
		}
		object.PartsIndex = &SimpleIndex{Index: sortedPartNum}
	}

	// To decrypt encryption key, we need to know IV first
	object.EncryptionKey, err = decryptSseKey(object.InitializationVector, object.EncryptionKey)
	if err != nil {
		return
	}

	object.Rowkey = rowkey
	// rowkey = BucketName + bigEndian(uint16(count("/", ObjectName)))
	// + ObjectName
	// + ObjectNameEnding
	// + bigEndian(uint64.max - unixNanoTimestamp)
	object.Name = string(rowkey[len(object.BucketName)+1 : len(rowkey)-9])
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
	helper.Debugln("ObjectFromResponse:", object)
	return
}

func ObjMapFromResponse(response *hrpc.Result) (objMap *ObjMap, err error) {
	objMap = new(ObjMap)
	for _, cell := range response.Cells {
		switch string(cell.Family) {
		case OBJMAP_COLUMN_FAMILY:
			switch string(cell.Qualifier) {
			case "nullVerNum":
				err = binary.Read(bytes.NewReader(cell.Value), binary.BigEndian,
					&objMap.NullVerNum)
				if err != nil {
					return
				}
			}
		}
	}
	timeData := []byte(strconv.FormatUint(objMap.NullVerNum, 10))
	objMap.NullVerId = hex.EncodeToString(xxtea.Encrypt(timeData, XXTEA_KEY))
	//helper.Debugln("ObjectFromResponse:", objMap)
	return
}

func (m *Meta) GetObject(bucketName string, objectName string, willNeed bool) (object *Object, err error) {
	getObject := func() (o interface{}, err error) {
		objectRowkeyPrefix, err := getObjectRowkeyPrefix(bucketName, objectName, "")
		if err != nil {
			return
		}
		prefixFilter := filter.NewPrefixFilter(objectRowkeyPrefix)
		stopKey := helper.CopiedBytes(objectRowkeyPrefix)
		stopKey[len(stopKey)-1]++
		ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
		defer done()
		scanRequest, err := hrpc.NewScanRangeStr(ctx, OBJECT_TABLE,
			string(objectRowkeyPrefix), string(stopKey),
			hrpc.Filters(prefixFilter), hrpc.NumberOfRows(1))
		if err != nil {
			return
		}
		scanResponse, err := m.Hbase.Scan(scanRequest)
		if err != nil {
			return
		}
		helper.Debugln("GetObject scanResponse length:", len(scanResponse))
		if len(scanResponse) == 0 {
			err = ErrNoSuchKey
			return
		}
		object, err := ObjectFromResponse(scanResponse[0])
		if err != nil {
			return
		}
		helper.Debugln("GetObject object.Name:", object.Name)
		if object.Name != objectName {
			err = ErrNoSuchKey
			return
		}
		return object, nil
	}
	unmarshaller := func(in []byte) (interface{}, error) {
		var object Object
		err := helper.MsgPackUnMarshal(in, &object)
		return &object, err
	}

	o, err := m.Cache.Get(redis.ObjectTable, bucketName+":"+objectName+":",
		getObject, unmarshaller, willNeed)
	if err != nil {
		return
	}
	object, ok := o.(*Object)
	if !ok {
		err = ErrInternalError
		return
	}
	return object, nil
}

func (m *Meta) GetAllObject(bucketName string, objectName string) (object []*Object, err error) {
	var objs []*Object
	objectRowkeyPrefix, err := getObjectRowkeyPrefix(bucketName, objectName, "")
	if err != nil {
		return nil, err
	}
	var exit bool
	startRowkey := objectRowkeyPrefix
	stopKey := helper.CopiedBytes(objectRowkeyPrefix)
	stopKey[len(stopKey)-1]++
	prefixFilter := filter.NewPrefixFilter(objectRowkeyPrefix)
	for !exit {
		ctx, _ := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
		//defer done() // TODO:

		scanRequest, err := hrpc.NewScanRangeStr(ctx, OBJECT_TABLE,
			string(startRowkey), string(stopKey),
			hrpc.Filters(prefixFilter), hrpc.NumberOfRows(ResponseNumberOfRows))
		if err != nil {
			helper.Logger.Printf(5, "Error new scan range str, err:", err)
			return nil, ErrInternalError
		}
		helper.Logger.Printf(20, "Start to call hbase scan:")
		scanResponse, err := m.Hbase.Scan(scanRequest)
		if err != nil {
			helper.Logger.Printf(5, "Error getting scan response, err:", err)
			return nil, ErrInternalError
		}
		if len(scanResponse) == 0 {
			break
		}

		for _, obj := range scanResponse {
			object, err := ObjectFromResponse(obj)
			if err != nil {
				helper.Logger.Printf(5, "Error converting response to object, err:", err)
				return nil, ErrInternalError
			}
			if object.Name != objectName {
				exit = true
				break
			}
			objs = append(objs, object)
			strRowkey, err := object.GetRowkey()
			if err != nil {
				helper.Logger.Printf(5, "Error getting row key for object, err:", err)
				return nil, ErrInternalError
			}
			startRowkey = []byte(strRowkey)
			helper.Logger.Println(20, "GetAllObject(): Row key:", startRowkey)
		}
		startRowkey[len(startRowkey)-1]++
		if len(scanResponse) != ResponseNumberOfRows {
			break
		}
	}

	if len(objs) == 0 {
		return nil, ErrNoSuchKey
	}
	return objs, nil
}

func (m *Meta) GetObjectMap(bucketName, objectName string) (objMap *ObjMap, err error) {
	objMapRowkeyPrefix, err := getObjectRowkeyPrefix(bucketName, objectName, "")
	if err != nil {
		return
	}
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	getRequest, err := hrpc.NewGetStr(ctx, OBJMAP_TABLE, string(objMapRowkeyPrefix))
	if err != nil {
		return
	}
	getResponse, err := m.Hbase.Get(getRequest)
	if err != nil {
		return
	}
	if len(getResponse.Cells) == 0 {
		err = ErrNoSuchKey
		return
	}
	objMap, err = ObjMapFromResponse(getResponse)
	if err != nil {
		return
	}
	objMap.BucketName = bucketName
	objMap.Name = objectName
	return
}

func (m *Meta) GetObjectVersion(bucketName, objectName, version string, willNeed bool) (object *Object, err error) {
	getObjectVersion := func() (o interface{}, err error) {
		objectRowkeyPrefix, err := getObjectRowkeyPrefix(bucketName, objectName, version)
		if err != nil {
			return
		}
		ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
		defer done()
		getRequest, err := hrpc.NewGetStr(ctx, OBJECT_TABLE, string(objectRowkeyPrefix))
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
		object, err := ObjectFromResponse(getResponse)
		if err != nil {
			return
		}
		if object.Name != objectName {
			err = ErrNoSuchKey
			return
		}
		return object, nil
	}
	unmarshaller := func(in []byte) (interface{}, error) {
		var object Object
		err := helper.MsgPackUnMarshal(in, &object)
		return &object, err
	}
	o, err := m.Cache.Get(redis.ObjectTable, bucketName+":"+objectName+":"+version,
		getObjectVersion, unmarshaller, willNeed)
	if err != nil {
		return
	}
	object, ok := o.(*Object)
	if !ok {
		err = ErrInternalError
		return
	}
	return object, nil
}

func (m *Meta) PutObjectEntry(object *Object) error {
	rowkey, err := object.GetRowkey()
	if err != nil {
		return err
	}
	values, err := object.GetValues()
	if err != nil {
		return err
	}
	helper.Debugln("values", values)
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	put, err := hrpc.NewPutStr(ctx, OBJECT_TABLE, rowkey, values)
	if err != nil {
		return err
	}
	_, err = m.Hbase.Put(put)
	return err
}

func (m *Meta) PutObjMapEntry(objMap *ObjMap) error {
	rowkey, err := objMap.GetRowKey()
	if err != nil {
		return err
	}
	values, err := objMap.GetValues()
	if err != nil {
		return err
	}
	helper.Debugln("values", values)
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	put, err := hrpc.NewPutStr(ctx, OBJMAP_TABLE, rowkey, values)
	if err != nil {
		return err
	}
	_, err = m.Hbase.Put(put)
	return err
}

func (m *Meta) DeleteObjectEntry(object *Object) error {
	rowkeyToDelete, err := object.GetRowkey()
	if err != nil {
		return err
	}
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	deleteRequest, err := hrpc.NewDelStr(ctx, OBJECT_TABLE, rowkeyToDelete,
		object.GetValuesForDelete())
	if err != nil {
		return err
	}
	_, err = m.Hbase.Delete(deleteRequest)
	return err
}

func (m *Meta) DeleteObjMapEntry(objMap *ObjMap) error {
	rowkeyToDelete, err := objMap.GetRowKey()
	if err != nil {
		return err
	}
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	deleteRequest, err := hrpc.NewDelStr(ctx, OBJMAP_TABLE, rowkeyToDelete,
		objMap.GetValuesForDelete())
	if err != nil {
		return err
	}
	_, err = m.Hbase.Delete(deleteRequest)
	return err
}

func decryptSseKey(initializationVector []byte, cipherText []byte) (plainText []byte, err error) {
	if len(cipherText) == 0 {
		return
	}

	block, err := aes.NewCipher(SSE_S3_MASTER_KEY)
	if err != nil {
		return
	}

	aesGcm, err := cipher.NewGCM(block)
	if err != nil {
		return
	}

	// InitializationVector is 16 bytes(because of CTR), but use only first 12 bytes in GCM
	// for performance
	return aesGcm.Open(nil, initializationVector[:12], cipherText, nil)
}
