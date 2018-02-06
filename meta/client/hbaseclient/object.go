package hbaseclient

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"github.com/cannium/gohbase/filter"
	"github.com/cannium/gohbase/hrpc"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/meta/util"
	"github.com/xxtea/xxtea-go/xxtea"
	"math"
	"strconv"
	"time"
)

func (h *HbaseClient) GetObject(bucketName, objectName, version string) (object *Object, err error) {
	objectRowkeyPrefix, err := getObjectRowkeyPrefix(bucketName, objectName, version)
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
	scanResponse, err := h.Client.Scan(scanRequest)
	if err != nil {
		return
	}
	helper.Debugln("GetObject scanResponse length:", len(scanResponse))
	if len(scanResponse) == 0 {
		err = ErrNoSuchKey
		return
	}
	object, err = ObjectFromResponse(scanResponse[0])
	return
}

func (h *HbaseClient) GetAllObject(bucketName, objectName, version string) (object []*Object, err error) {
	var objs []*Object
	objectRowkeyPrefix, err := getObjectRowkeyPrefix(bucketName, objectName, version)
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
		scanResponse, err := h.Client.Scan(scanRequest)
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

func (h *HbaseClient) PutObject(object *Object) error {
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
	_, err = h.Client.Put(put)
	return err
}

func (h *HbaseClient) DeleteObject(object *Object) error {
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
	_, err = h.Client.Delete(deleteRequest)
	return err
}

//util func
// Rowkey format:
// BucketName + ObjectNameSeparator + ObjectName + ObjectNameSeparator +
// bigEndian(uint64.max - unixNanoTimestamp)
// The prefix excludes timestamp part if version is empty
func getObjectRowkeyPrefix(bucketName string, objectName string, version string) ([]byte, error) {
	var rowkey bytes.Buffer
	rowkey.WriteString(bucketName + ObjectNameSeparator)
	rowkey.WriteString(objectName + ObjectNameSeparator)
	if version != "" {
		decrypted, err := util.Decrypt(version)
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
			//              p.Etag = ""         // The member is not used, so give it null value
			//              p.LastModified = "" // The member is not used, so give it null value
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
