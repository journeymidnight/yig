package tidbclient

import (
	"database/sql"
	"encoding/json"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/meta/util"
)

func (t *TidbClient) GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error) {
	multipart.Parts = make(map[int]*Part)
	timestampString, err := util.Decrypt(uploadId)
	if err != nil {
		return
	}
	uploadTime, err := strconv.ParseUint(timestampString, 10, 64)
	if err != nil {
		return
	}
	uploadTime = math.MaxUint64 - uploadTime
	sqltext := "select bucketname,objectname,uploadtime,initiatorid,ownerid,contenttype,location,pool,acl,sserequest," +
		"encryption,COALESCE(cipher,\"\"),attrs,storageclass from multiparts where bucketname=? and objectname=? and uploadtime=?;"
	var initialTime uint64
	var acl, sseRequest, attrs string
	err = t.Client.QueryRow(sqltext, bucketName, objectName, uploadTime).Scan(
		&multipart.BucketName,
		&multipart.ObjectName,
		&initialTime,
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
		return
	}
	rinitial := int64(math.MaxUint64 - initialTime)
	s := rinitial / 1e9
	ns := rinitial % 1e9
	multipart.InitialTime = time.Unix(s, ns)
	err = json.Unmarshal([]byte(acl), &multipart.Metadata.Acl)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(sseRequest), &multipart.Metadata.SseRequest)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(attrs), &multipart.Metadata.Attrs)
	if err != nil {
		return
	}

	sqltext = "select partnumber,size,objectid,offset,etag,lastmodified,initializationvector from multipartpart where bucketname=? and objectname=? and uploadtime=?;"
	rows, err := t.Client.Query(sqltext, bucketName, objectName, uploadTime)
	if err != nil {
		return
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
		ts, e := time.Parse(TIME_LAYOUT_TIDB, p.LastModified)
		if e != nil {
			return
		}
		p.LastModified = ts.Format(CREATE_TIME_LAYOUT)
		multipart.Parts[p.PartNumber] = p
		if err != nil {
			return
		}
	}
	return
}

func (t *TidbClient) CreateMultipart(multipart Multipart) (err error) {
	m := multipart.Metadata
	uploadtime := math.MaxUint64 - uint64(multipart.InitialTime.UnixNano())
	acl, _ := json.Marshal(m.Acl)
	sseRequest, _ := json.Marshal(m.SseRequest)
	attrs, _ := json.Marshal(m.Attrs)
	sqltext := "insert into multiparts(bucketname,objectname,uploadtime,initiatorid,ownerid,contenttype,location,pool,acl,sserequest,encryption,cipher,attrs,storageclass) " +
		"values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	_, err = t.Client.Exec(sqltext, multipart.BucketName, multipart.ObjectName, uploadtime, m.InitiatorId, m.OwnerId, m.ContentType, m.Location, m.Pool, acl, sseRequest, m.EncryptionKey,m.CipherKey, attrs, m.StorageClass)
	return
}

func (t *TidbClient) PutObjectPart(multipart *Multipart, part *Part, tx DB) (err error) {
	if tx == nil {
		tx = t.Client
	}

	uploadtime := math.MaxUint64 - uint64(multipart.InitialTime.UnixNano())
	lastt, err := time.Parse(CREATE_TIME_LAYOUT, part.LastModified)
	if err != nil {
		return
	}
	lastModified := lastt.Format(TIME_LAYOUT_TIDB)
	sqltext := "insert into multipartpart(partnumber,size,objectid,offset,etag,lastmodified,initializationvector,bucketname,objectname,uploadtime) " +
		"values(?,?,?,?,?,?,?,?,?,?)"
	_, err = tx.Exec(sqltext, part.PartNumber, part.Size, part.ObjectId, part.Offset, part.Etag, lastModified, part.InitializationVector, multipart.BucketName, multipart.ObjectName, uploadtime)
	return
}

func (t *TidbClient) DeleteMultipart(multipart *Multipart, tx DB) (err error) {
	if tx == nil {
		tx, err = t.Client.Begin()
		if err != nil {
			return err
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
	uploadtime := math.MaxUint64 - uint64(multipart.InitialTime.UnixNano())
	sqltext := "delete from multiparts where bucketname=? and objectname=? and uploadtime=?;"
	_, err = tx.Exec(sqltext, multipart.BucketName, multipart.ObjectName, uploadtime)
	if err != nil {
		return
	}
	sqltext = "delete from multipartpart where bucketname=? and objectname=? and uploadtime=?;"
	_, err = tx.Exec(sqltext, multipart.BucketName, multipart.ObjectName, uploadtime)
	return err
}

func (t *TidbClient) ListMultipartUploads(bucketName, keyMarker, uploadIdMarker, prefix, delimiter, encodingType string, maxUploads int) (uploads []datatype.Upload, prefixs []string, isTruncated bool, nextKeyMarker, nextUploadIdMarker string, err error) {
	var count int
	var exit bool
	commonPrefixes := make(map[string]struct{})
	var uploadNum uint64
	if uploadIdMarker != "" {
		uploadNum, err = strconv.ParseUint(uploadIdMarker, 10, 64)
	}
	if err != nil {
		return
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
			return
		}
		defer rows.Close()
		for rows.Next() {
			loopnum += 1
			var name, initiatorid, ownerid string
			var uploadtime uint64
			var storageClass StorageClass
			err = rows.Scan(
				&name,
				&uploadtime,
				&initiatorid,
				&ownerid,
				&storageClass,
			)
			if err != nil {
				return
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
				nextUploadIdMarker = GetMultipartUploadIdForTidb(uploadtime)
				exit = true
				break
			}
			upload.UploadId = GetMultipartUploadIdForTidb(uploadtime)
			upload.Key = name
			if encodingType != "" {
				upload.Key = url.QueryEscape(upload.Key)
			}
			var user common.Credential
			user, err = iam.GetCredentialByUserId(ownerid)
			if err != nil {
				return
			}
			upload.Owner.ID = user.UserId
			upload.Owner.DisplayName = user.DisplayName
			user, err = iam.GetCredentialByUserId(initiatorid)
			if err != nil {
				return
			}
			upload.Initiator.ID = user.UserId
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
	prefixs = helper.Keys(commonPrefixes)
	return
}

func (t *TidbClient) RenameObjectPart(object *Object, sourceObject string, tx DB) (err error) {
	if tx == nil {
		tx = t.Client
	}
	sql, args := object.GetUpdateObjectPartNameSql(sourceObject)
	_, err = tx.Exec(sql, args...)
	return err
}
