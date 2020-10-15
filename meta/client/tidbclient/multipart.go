package tidbclient

import (
	"database/sql"
	. "database/sql/driver"
	"encoding/json"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	meta "github.com/journeymidnight/yig/meta/common"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error) {
	multipart.Parts = make(map[int]*Part)
	initialTime, err := GetInitialTimeFromUploadId(uploadId)
	if err != nil {
		return
	}
	uploadTime := math.MaxUint64 - initialTime
	sqltext := "select bucketname,objectname,uploadtime,initiatorid,ownerid,contenttype,location,pool,acl,sserequest," +
		"encryption,COALESCE(cipher,\"\"),attrs,storageclass from multiparts where bucketname=? and objectname=? and uploadtime=?;"
	var acl, sseRequest, attrs string
	err = t.Client.QueryRow(sqltext, bucketName, objectName, uploadTime).Scan(
		&multipart.BucketName,
		&multipart.ObjectName,
		&multipart.InitialTime,
		&multipart.Metadata.InitiatorId,
		&multipart.Metadata.OwnerId,
		&multipart.Metadata.ContentType,
		&multipart.Metadata.Location,
		&multipart.Metadata.Pool,
		&acl,
		&sseRequest,
		&multipart.Metadata.EncryptionKey,
		&multipart.Metadata.CipherKey,
		&attrs,
		&multipart.Metadata.StorageClass,
	)
	if err != nil && err == sql.ErrNoRows {
		err = ErrNoSuchUpload
		return
	} else if err != nil {
		return multipart, NewError(InTidbFatalError, "GetMultipart scan row err", err)
	}

	multipart.InitialTime = math.MaxUint64 - multipart.InitialTime
	err = json.Unmarshal([]byte(acl), &multipart.Metadata.Acl)
	if err != nil {
		return multipart, NewError(InTidbFatalError, "GetMultipart unmarshal acl err", err)
	}
	err = json.Unmarshal([]byte(sseRequest), &multipart.Metadata.SseRequest)
	if err != nil {
		return multipart, NewError(InTidbFatalError, "GetMultipart unmarshal sseRequest err", err)
	}
	err = json.Unmarshal([]byte(attrs), &multipart.Metadata.Attrs)
	if err != nil {
		return multipart, NewError(InTidbFatalError, "GetMultipart unmarshal attrs err", err)
	}

	sqltext = "select partnumber,size,objectid,offset,etag,lastmodified,initializationvector from multipartpart where bucketname=? and objectname=? and uploadtime=?;"
	rows, err := t.Client.Query(sqltext, bucketName, objectName, uploadTime)
	if err != nil {
		return multipart, NewError(InTidbFatalError, "GetMultipart query err", err)
	}
	defer rows.Close()
	for rows.Next() {
		p := &Part{}
		err = rows.Scan(
			&p.PartNumber,
			&p.Size,
			&p.ObjectId,
			&p.Offset,
			&p.Etag,
			&p.LastModified,
			&p.InitializationVector,
		)
		if err != nil {
			return multipart, NewError(InTidbFatalError, "GetMultipart scan row err", err)
		}
		ts, e := time.Parse(TIME_LAYOUT_TIDB, p.LastModified)
		if e != nil {
			return multipart, NewError(InTidbFatalError, "GetMultipart parse time err", err)
		}
		p.LastModified = ts.Format(CREATE_TIME_LAYOUT)
		multipart.Parts[p.PartNumber] = p
	}
	return
}

func (t *TidbClient) CreateMultipart(multipart Multipart) (err error) {
	m := multipart.Metadata
	uploadtime := math.MaxUint64 - multipart.InitialTime
	acl, _ := json.Marshal(m.Acl)
	sseRequest, _ := json.Marshal(m.SseRequest)
	attrs, _ := json.Marshal(m.Attrs)
	sqltext := "insert into multiparts(bucketname,objectname,uploadtime,initiatorid,ownerid,contenttype,location,pool,acl,sserequest,encryption,cipher,attrs,storageclass) " +
		"values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	_, err = t.Client.Exec(sqltext, multipart.BucketName, multipart.ObjectName, uploadtime, m.InitiatorId, m.OwnerId, m.ContentType, m.Location, m.Pool, acl, sseRequest, m.EncryptionKey, m.CipherKey, attrs, m.StorageClass)
	if err != nil {
		return NewError(InTidbFatalError, "CreateMultipart transaction executes err", err)
	}
	return nil
}

func (t *TidbClient) PutObjectPart(multipart *Multipart, part *Part) (deltaSize int64, err error) {
	tx, err := t.Client.Begin()
	if err != nil {
		return 0, NewError(InTidbFatalError, "PutObjectPart transaction starts err", err)
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		}
		if err != nil {
			tx.Rollback()
		}
	}()
	uploadtime := math.MaxUint64 - multipart.InitialTime
	lastt, err := time.Parse(CREATE_TIME_LAYOUT, part.LastModified)
	if err != nil {
		return 0, NewError(InTidbFatalError, "PutObjectPart parse time err", err)
	}
	lastModified := lastt.Format(TIME_LAYOUT_TIDB)
	sqltext := "insert into multipartpart(partnumber,size,objectid,offset,etag,lastmodified,initializationvector,bucketname,objectname,uploadtime) " +
		"values(?,?,?,?,?,?,?,?,?,?)"

	_, err = tx.Exec(sqltext, part.PartNumber, part.Size, part.ObjectId, part.Offset, part.Etag, lastModified, part.InitializationVector, multipart.BucketName, multipart.ObjectName, uploadtime)
	var removedSize int64 = 0
	if part, ok := multipart.Parts[part.PartNumber]; ok {
		removedSize += part.Size
	}
	deltaSize = part.Size - removedSize
	err = t.UpdateUsage(multipart.BucketName, deltaSize, tx)
	if err != nil {
		return 0,NewError(InTidbFatalError, "PutObjectPart transaction executes err", err)
	}
	return deltaSize,nil
}

func (t *TidbClient) DeleteMultipart(multipart *Multipart, tx Tx) (err error) {
	if tx == nil {
		tx, err = t.Client.Begin()
		if err != nil {
			return NewError(InTidbFatalError, "DeleteMultipart transaction starts err", err)
		}
		defer func() {
			if err == nil {
				err = tx.(*sql.Tx).Commit()
			}
			if err != nil {
				tx.(*sql.Tx).Rollback()
			}
		}()
	}
	uploadtime := math.MaxUint64 - multipart.InitialTime
	sqltext := "delete from multiparts where bucketname=? and objectname=? and uploadtime=?;"
	_, err = tx.(*sql.Tx).Exec(sqltext, multipart.BucketName, multipart.ObjectName, uploadtime)
	if err != nil {
		return NewError(InTidbFatalError, "DeleteMultipart transaction executes err", err)
	}
	sqltext = "delete from multipartpart where bucketname=? and objectname=? and uploadtime=?;"
	_, err = tx.(*sql.Tx).Exec(sqltext, multipart.BucketName, multipart.ObjectName, uploadtime)
	if err != nil {
		return NewError(InTidbFatalError, "DeleteMultipart transaction executes err", err)
	}
	return nil
}

func (t *TidbClient) ListMultipartUploads(bucketName, keyMarker, uploadIdMarker, prefix, delimiter, encodingType string, maxUploads int) (result datatype.ListMultipartUploadsResponse, err error) {
	var count int
	var exit, isTruncated bool
	var nextKeyMarker, nextUploadIdMarker string
	var uploads []datatype.Upload
	var prefixes []string
	commonPrefixes := make(map[string]struct{})
	var uploadNum uint64
	if uploadIdMarker != "" {
		uploadNum, err = strconv.ParseUint(uploadIdMarker, 10, 64)
	}
	if err != nil {
		return result, NewError(InTidbFatalError, "ListMultipartUploads parse uploadIdMarker err", err)
	}
	var objnum map[string]int = make(map[string]int)
	var currentMarker string = keyMarker
	var first bool = true

	for {
		var loopnum int
		if _, ok := objnum[currentMarker]; !ok {
			objnum[currentMarker] = 0
		}
		var sqltext string
		var rows *sql.Rows
		if currentMarker == "" {
			sqltext = "select objectname,uploadtime,initiatorid,ownerid,storageclass from multiparts where bucketName=? order by bucketname,objectname,uploadtime limit ?,?;"
			rows, err = t.Client.Query(sqltext, bucketName, objnum[currentMarker], objnum[currentMarker]+maxUploads)
		} else {
			sqltext = "select objectname,uploadtime,initiatorid,ownerid,storageclass from multiparts where bucketName=? and objectname>=? order by bucketname,objectname,uploadtime limit ?,?;"
			rows, err = t.Client.Query(sqltext, bucketName, currentMarker, objnum[currentMarker], objnum[currentMarker]+maxUploads)
		}
		if err != nil {
			return result, NewError(InTidbFatalError, "ListMultipartUploads query err", err)
		}
		defer rows.Close()
		for rows.Next() {
			loopnum += 1
			var name, initiatorid, ownerid string
			var uploadtime uint64
			var storageClass meta.StorageClass
			err = rows.Scan(
				&name,
				&uploadtime,
				&initiatorid,
				&ownerid,
				&storageClass,
			)
			if err != nil {
				return result, NewError(InTidbFatalError, "ListMultipartUploads scan row err", err)
			}
			if _, ok := objnum[name]; !ok {
				objnum[name] = 0
			}
			objnum[name] += 1
			currentMarker = name
			upload := datatype.Upload{StorageClass: storageClass.ToString()}
			//filte by uploadtime and key
			if first {
				if uploadNum != 0 {
					if name == currentMarker && uploadtime < uploadNum {
						continue
					}
				}
			}
			//filte by prefix
			hasPrefix := strings.HasPrefix(name, prefix)
			if !hasPrefix {
				continue
			}
			//filte by delimiter
			if len(delimiter) != 0 {
				subStr := strings.TrimPrefix(name, prefix)
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := string([]byte(subStr)[0:(n + 1)])
					if _, ok := commonPrefixes[prefixKey]; !ok {
						commonPrefixes[prefixKey] = struct{}{}
					}
					continue
				}
			}
			if count >= maxUploads {
				isTruncated = true
				exit = true
				nextKeyMarker = name
				nextUploadIdMarker = GetMultipartUploadIdByDbTime(uploadtime)
				exit = true
				break
			}
			upload.UploadId = GetMultipartUploadIdByDbTime(uploadtime)
			upload.Key = name
			if encodingType != "" {
				upload.Key = url.QueryEscape(upload.Key)
			}
			var user common.Credential
			user, err = iam.GetCredentialByUserId(ownerid)
			if err != nil {
				return result, NewError(InTidbFatalError, "ListMultipartUploads GetCredentialByUserId err", err)
			}
			upload.Owner.ID = user.ExternUserId
			upload.Owner.DisplayName = user.DisplayName
			user, err = iam.GetCredentialByUserId(initiatorid)
			if err != nil {
				return result, NewError(InTidbFatalError, "ListMultipartUploads GetCredentialByUserId err", err)
			}
			upload.Initiator.ID = user.ExternUserId
			upload.Initiator.DisplayName = user.DisplayName
			timestamp := int64(math.MaxUint64 - uploadtime)
			s := timestamp / 1e9
			ns := timestamp % 1e9
			upload.Initiated = time.Unix(s, ns).UTC().Format(CREATE_TIME_LAYOUT)
			uploads = append(uploads, upload)
			count += 1
		}
		if loopnum == 0 {
			exit = true
		}
		first = false
		if exit {
			break
		}
	}
	prefixes = helper.Keys(commonPrefixes)
	result.IsTruncated = isTruncated
	result.Uploads = uploads
	result.NextKeyMarker = nextKeyMarker
	result.NextUploadIdMarker = nextUploadIdMarker

	sort.Strings(prefixes)
	for _, prefix := range prefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, datatype.CommonPrefix{
			Prefix: prefix,
		})
	}

	result.Bucket = bucketName
	result.Delimiter = delimiter
	result.KeyMarker = keyMarker
	result.UploadIdMarker = uploadIdMarker
	result.MaxUploads = maxUploads
	result.Prefix = prefix

	return
}
