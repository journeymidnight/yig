package hbaseclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"github.com/cannium/gohbase/filter"
	"github.com/cannium/gohbase/hrpc"
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/meta/util"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func (h *HbaseClient) GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error) {
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
	getMultipartResponse, err := h.Client.Get(getMultipartRequest)
	if err != nil {
		return
	}
	if len(getMultipartResponse.Cells) == 0 {
		err = ErrNoSuchUpload
		return
	}
	return MultipartFromResponse(getMultipartResponse, bucketName)
}

func (h *HbaseClient) CreateMultipart(multipart Multipart) (err error) {
	multipartValues, err := multipart.GetValues()
	if err != nil {
		return
	}
	rowkey, err := multipart.GetRowkey()
	if err != nil {
		return
	}
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	newMultipartPut, err := hrpc.NewPutStr(ctx, MULTIPART_TABLE,
		rowkey, multipartValues)
	if err != nil {
		return
	}
	_, err = h.Client.Put(newMultipartPut)
	return err
}

func (h *HbaseClient) PutObjectPart(multipart Multipart, part Part) (err error) {
	partValues, err := part.GetValues()
	if err != nil {
		return
	}
	rowkey, err := multipart.GetRowkey()
	if err != nil {
		return
	}
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	partMetaPut, err := hrpc.NewPutStr(ctx, MULTIPART_TABLE, rowkey, partValues)
	if err != nil {
		return
	}
	_, err = h.Client.Put(partMetaPut)
	return
}

func (h *HbaseClient) DeleteMultipart(multipart Multipart) (err error) {
	deleteValues := multipart.GetValuesForDelete()
	rowkey, err := multipart.GetRowkey()
	if err != nil {
		return
	}
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	deleteRequest, err := hrpc.NewDelStr(ctx, MULTIPART_TABLE, rowkey, deleteValues)
	if err != nil {
		return
	}
	_, err = h.Client.Delete(deleteRequest)
	return
}

func (h *HbaseClient) ListMultipartUploads(bucketName, keyMarker, uploadIdMarker, prefix, delimiter, encodingType string, maxUploads int) (uploads []datatype.Upload, prefixs []string, isTruncated bool, nextKeyMarker, nextUploadIdMarker string, err error) {

	var startRowkey bytes.Buffer
	var stopKey []byte
	startRowkey.WriteString(bucketName)
	stopKey = helper.CopiedBytes(startRowkey.Bytes())
	// TODO: refactor, same as in getMultipartRowkeyFromUploadId
	if keyMarker != "" {
		err = binary.Write(&startRowkey, binary.BigEndian,
			uint16(strings.Count(keyMarker, "/")))
		if err != nil {
			return
		}
		startRowkey.WriteString(keyMarker)
		stopKey = helper.CopiedBytes(startRowkey.Bytes())
		if uploadIdMarker != "" {
			var timestampString string
			timestampString, err = util.Decrypt(uploadIdMarker)
			if err != nil {
				return
			}
			var timestamp uint64
			timestamp, err = strconv.ParseUint(timestampString, 10, 64)
			if err != nil {
				return
			}
			err = binary.Write(&startRowkey, binary.BigEndian, timestamp)
			if err != nil {
				return
			}
		}
	}
	stopKey[len(stopKey)-1]++

	comparator := filter.NewRegexStringComparator(
		"^"+bucketName+".."+prefix+".*"+".{8}"+"$",
		0x20, // Dot-all mode
		"ISO-8859-1",
		"JAVA", // regexp engine name, in `JAVA` or `JONI`
	)
	compareFilter := filter.NewCompareFilter(filter.Equal, comparator)
	rowFilter := filter.NewRowFilter(compareFilter)

	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	scanRequest, err := hrpc.NewScanRangeStr(ctx, MULTIPART_TABLE,
		startRowkey.String(), string(stopKey), hrpc.Filters(rowFilter),
		// scan for max+1 rows to determine if results are truncated
		hrpc.NumberOfRows(uint32(maxUploads+1)))
	if err != nil {
		return
	}
	scanResponse, err := h.Client.Scan(scanRequest)
	if err != nil {
		return
	}

	if len(scanResponse) > maxUploads {
		isTruncated = true
		var nextUpload Multipart
		nextUpload, err = MultipartFromResponse(scanResponse[maxUploads], bucketName)
		if err != nil {
			return
		}
		nextKeyMarker = nextUpload.ObjectName
		nextUploadIdMarker, err = nextUpload.GetUploadId()
		if err != nil {
			return
		}
		scanResponse = scanResponse[:maxUploads]
	}

	var currentLevel int
	if delimiter == "" {
		currentLevel = 0
	} else {
		currentLevel = strings.Count(prefix, delimiter)
	}

	uploads = make([]datatype.Upload, 0, len(scanResponse))
	prefixMap := make(map[string]int) // value is dummy, only need a set here
	for _, row := range scanResponse {
		var m Multipart
		m, err = MultipartFromResponse(row, bucketName)
		if err != nil {
			return
		}
		upload := datatype.Upload{
			StorageClass: "STANDARD",
			Initiated:    m.InitialTime.UTC().Format(CREATE_TIME_LAYOUT),
		}
		if delimiter == "" {
			upload.Key = m.ObjectName
		} else {
			level := strings.Count(m.ObjectName, delimiter)
			if level > currentLevel {
				split := strings.Split(m.ObjectName, delimiter)
				split = split[:currentLevel+1]
				prefix := strings.Join(split, delimiter) + delimiter
				prefixMap[prefix] = 1
				continue
			} else {
				upload.Key = m.ObjectName
			}
		}
		//upload.Key = strings.TrimPrefix(upload.Key, prefix)
		if encodingType != "" { // only support "url" encoding for now
			upload.Key = url.QueryEscape(upload.Key)
		}
		upload.UploadId, err = m.GetUploadId()
		if err != nil {
			return
		}

		var user iam.Credential
		user, err = iam.GetCredentialByUserId(m.Metadata.OwnerId)
		if err != nil {
			return
		}
		upload.Owner.ID = user.UserId
		upload.Owner.DisplayName = user.DisplayName
		user, err = iam.GetCredentialByUserId(m.Metadata.InitiatorId)
		if err != nil {
			return
		}
		upload.Initiator.ID = user.UserId
		upload.Initiator.DisplayName = user.DisplayName

		uploads = append(uploads, upload)
	}
	uploads = uploads

	prefixs = helper.Keys(prefixMap)
	return
}

func getMultipartRowkeyFromUploadId(bucketName, objectName, uploadId string) (string, error) {
	var rowkey bytes.Buffer
	rowkey.WriteString(bucketName)
	err := binary.Write(&rowkey, binary.BigEndian, uint16(strings.Count(objectName, "/")))
	if err != nil {
		return "", err
	}
	rowkey.WriteString(objectName)
	timestampString, err := util.Decrypt(uploadId)
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
