package hbaseclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"github.com/cannium/gohbase/filter"
	"github.com/cannium/gohbase/hrpc"
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/xxtea/xxtea-go/xxtea"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func (h *HbaseClient) GetBucket(bucketName string) (bucket Bucket, err error) {

	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	getRequest, err := hrpc.NewGetStr(ctx, BUCKET_TABLE, bucketName)
	if err != nil {
		return
	}
	response, err := h.Client.Get(getRequest)
	if err != nil {
		return
	}
	if len(response.Cells) == 0 {
		err = ErrNoSuchBucket
		return
	}
	for _, cell := range response.Cells {
		switch string(cell.Qualifier) {
		case "createTime":
			bucket.CreateTime, err = time.Parse(CREATE_TIME_LAYOUT, string(cell.Value))
			if err != nil {
				return
			}
		case "UID":
			bucket.OwnerId = string(cell.Value)
		case "CORS":
			var cors datatype.Cors
			err = json.Unmarshal(cell.Value, &cors)
			if err != nil {
				return
			}
			bucket.CORS = cors
		case "LC":
			var lc datatype.Lc
			err = json.Unmarshal(cell.Value, &lc)
			if err != nil {
				return
			}
			bucket.LC = lc
		case "ACL":
			bucket.ACL.CannedAcl = string(cell.Value)
		case "versioning":
			bucket.Versioning = string(cell.Value)
		case "usage":
			err = binary.Read(bytes.NewReader(cell.Value), binary.BigEndian,
				&bucket.Usage)
			if err != nil {
				return
			}
		default:
		}
	}
	bucket.Name = bucketName
	return
}

func (h *HbaseClient) PutBucket(bucket Bucket) error {
	values, err := bucket.GetValues()
	if err != nil {
		return err
	}
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	put, err := hrpc.NewPutStr(ctx, BUCKET_TABLE, bucket.Name, values)
	if err != nil {
		return err
	}
	_, err = h.Client.Put(put)
	return err
}

func (h *HbaseClient) CheckAndPutBucket(bucket Bucket) (bool, error) {
	values, err := bucket.GetValues()
	if err != nil {
		return false, err
	}
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	put, err := hrpc.NewPutStr(ctx, BUCKET_TABLE, bucket.Name, values)
	if err != nil {
		return false, err
	}
	processed, err := h.Client.CheckAndPut(put, BUCKET_COLUMN_FAMILY,
		"UID", []byte{})
	return processed, err
}

func (h *HbaseClient) DeleteBucket(bucket Bucket) error {
	values, err := bucket.GetValues()
	if err != nil {
		return err
	}
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	deleteRequest, err := hrpc.NewDelStr(ctx, BUCKET_TABLE, bucket.Name, values)
	if err != nil {
		return err
	}
	_, err = h.Client.Delete(deleteRequest)

	return err
}

func (h *HbaseClient) UpdateUsage(bucketName string, size int64) {
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	inc, err := hrpc.NewIncStrSingle(ctx, BUCKET_TABLE, bucketName,
		BUCKET_COLUMN_FAMILY, "usage", size)
	retValue, err := h.Client.Increment(inc)
	if err != nil {
		helper.Logger.Println(5, "Inconsistent data: usage of bucket", bucketName,
			"should add by", size)
	}
	helper.Debugln("New usage:", retValue)
}

func (h *HbaseClient) ListObjects(bucketName, marker, verIdMarker, prefix, delimiter string, versioned bool, maxKeys int) (retObjects []*Object, prefixes []string, truncated bool, nextMarker, nextVerIdMarker string, err error) {
	var exit bool
	var count int
	truncated = true
	var currMarker string
	currMarker = marker
	var currVerMarkerNum uint64
	if verIdMarker == "null" {
		objMap, e := h.GetObjectMap(bucketName, marker)
		if e != nil {
			err = e
			return
		}
		verIdMarker = objMap.NullVerId
	}
	if verIdMarker != "" {
		var versionBytes []byte
		versionBytes, err = hex.DecodeString(verIdMarker)
		if err == nil {
			decrypted := xxtea.Decrypt(versionBytes, XXTEA_KEY)
			unixNanoTimestamp, e := strconv.ParseUint(string(decrypted), 10, 64)
			if e != nil {
				helper.Debugln("Error convert version id to int")
				err = ErrInvalidVersioning
				return
			}
			currVerMarkerNum = unixNanoTimestamp
		} else {
			err = nil
			helper.Debugln("Error decoding version id, skip to next object")
			currVerMarkerNum = 0
		}
	}
	var biggerThanDelim string
	var skipAfterDelim string
	var skipOldVerObj string
	objectMap := make(map[string]*Object)
	commonPrefixes := make(map[string]bool)
	if len(delimiter) != 0 {
		r, _ := utf8.DecodeRune([]byte(delimiter))
		r = r + 1
		buf := make([]byte, 3)
		utf8.EncodeRune(buf, r)
		biggerThanDelim = string(buf)
		helper.Debugln("list objects, biggerThanDelim:", biggerThanDelim)
	}

	var newMarker bool
	if len(delimiter) != 0 && len(prefix) < len(currMarker) {
		len := len(prefix)
		subStr := currMarker[len:]
		idx := strings.Index(subStr, delimiter)
		if idx != -1 {
			newMarker = true
			currMarker = currMarker[0:(len + idx)]
			currMarker += biggerThanDelim
			currVerMarkerNum = 0
			helper.Debugln("sub:", subStr, "len", len, "idx", idx, "currMarker", currMarker)
		}
	}
	if currMarker != "" && !newMarker {
		if !versioned || currVerMarkerNum == 0 {
			currMarker += ObjectNameSmallestStr
		} else {
			currVerMarkerNum -= 1
		}
	}

	for truncated && count <= maxKeys {
		// Because start rowkey is included in scan result, update currMarker
		if strings.Compare(skipAfterDelim, currMarker) > 0 {
			currMarker = skipAfterDelim
			currVerMarkerNum = 0
			helper.Debugln("set new currMarker:", currMarker)
		}
		if strings.Compare(skipOldVerObj, currMarker) > 0 {
			currMarker = skipOldVerObj
			currVerMarkerNum = 0
			helper.Debugln("set new currMarker:", currMarker)
		}

		var startRowkey bytes.Buffer
		startRowkey.WriteString(bucketName + ObjectNameSeparator)
		if currMarker != "" {
			startRowkey.WriteString(currMarker)
		}
		if currVerMarkerNum != 0 {
			startRowkey.WriteString(ObjectNameSeparator)
			err = binary.Write(&startRowkey, binary.BigEndian,
				math.MaxUint64-currVerMarkerNum)
			if err != nil {
				return
			}
		}
		stopKey := []byte(bucketName)
		stopKey[len(bucketName)-1]++
		comparator := filter.NewRegexStringComparator(
			"^"+bucketName+ObjectNameSeparator+prefix+".*",
			0x20, // Dot-all mode
			"UTF-8",
			"JAVA", // regexp engine name, in `JAVA` or `JONI`
		)
		compareFilter := filter.NewCompareFilter(filter.Equal, comparator)
		rowFilter := filter.NewRowFilter(compareFilter)

		ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
		defer done()
		scanRequest, e := hrpc.NewScanRangeStr(ctx, OBJECT_TABLE,
			startRowkey.String(), string(stopKey),
			// scan for max+1 rows to determine if results are truncated
			hrpc.Filters(rowFilter), hrpc.NumberOfRows(uint32(maxKeys+1)))
		if e != nil {
			err = e
			return
		}
		scanResponse, e := h.Client.Scan(scanRequest)
		if e != nil {
			err = e
			return
		}
		if len(scanResponse) > 0 {
			if len(scanResponse) > maxKeys {
				var lstObject *Object
				lstObject, err = ObjectFromResponse(scanResponse[maxKeys])
				if err != nil {
					return
				}
				currMarker = lstObject.Name
				if versioned {
					currVerMarkerNum, err = lstObject.GetVersionNumber()
					if err != nil {
						return
					}
				}

				scanResponse = scanResponse[0 : maxKeys+1]
				truncated = true
			} else {
				truncated = false
			}
		} else {
			truncated = false
			exit = true
		}
		// search objects
		var idx int
		var row *hrpc.Result
		for idx, row = range scanResponse {
			var o *Object
			o, e = ObjectFromResponse(row)
			if e != nil {
				err = e
				return
			}
			if _, ok := objectMap[o.Name]; !ok {
				objectMap[o.Name] = o
				if o.DeleteMarker && !versioned {
					continue
				}
			} else {
				if !versioned {
					skipOldVerObj = o.Name + ObjectNameSmallestStr
					continue
				}
			}
			if count < maxKeys {
				//request.Marker = o.Name
				nextMarker = o.Name
				if versioned {
					nextVerIdMarker = o.VersionId
				}
			}

			if len(delimiter) != 0 {
				objName := o.Name
				len := len(prefix)
				subStr := objName[len:]
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := string([]rune(objName)[0:(len + n + 1)])
					if _, ok := commonPrefixes[prefixKey]; !ok {
						if count >= maxKeys {
							truncated = true
							exit = true
							break
						}
						nextMarker = prefixKey
						commonPrefixes[prefixKey] = true

						skipAfterDelim = objName[0:(len + n)]
						skipAfterDelim += biggerThanDelim
						helper.Debugln("skipAfterDelim:", skipAfterDelim)
						count += 1
					}
					continue
				}
			}

			if count >= maxKeys {
				truncated = true
				exit = true
				break
			}

			retObjects = append(retObjects, o)
			count += 1
		}
		if exit {
			break
		}
		truncated = truncated || (idx+1 != len(scanResponse))
	}
	prefixes = helper.Keys(commonPrefixes)
	return
}
