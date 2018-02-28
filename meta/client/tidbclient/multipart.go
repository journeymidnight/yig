package tidbclient

import (
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error) {
	timestampString, err := util.Decrypt(uploadId)
	if err != nil {
		return
	}
	uploadTime, err := strconv.ParseUint(timestampString, 10, 64)
	if err != nil {
		return
	}

	sqltext := fmt.Sprintf("select * from multiparts where bucketname='%s' and objectname='%s' and upploadtime=%d ", bucketName, objectName, uploadTime)
	var initialTime uint64
	var acl, sseRequest, attrs string
	err = t.Client.QueryRow(sqltext).Scan(
		&mutipart.BucketName,
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
		&attrs,
	)
	if err != nil && err == sql.ErrNoRows {
		err = ErrNoSuchUpload
		return
	} else if err != nil {
		return
	}
	multipart.InitialTime, err = time.Parse(TIME_LAYOUT_TIDB, strconv.FormatUint(initialTime, 10))
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

	sqltext = fmt.Sprintf("select partnumber,size,objectid,offset,etag,lastmodified,initializationvector from multipartpart where bucketname='%s' and objectname='%s' and upploadtime=%d ", bucketName, objectName, uploadTime)
	rows := t.Client.Query()
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
		multipart.Parts[p.PartNumber] = p
		if err != nil {
			return
		}
	}
	return
}

func (t *TidbClient) CreateMultipart(multipart Multipart) (err error) {
	uploadtime := uint64(m.InitialTime.UnixNano())
	m := multipart.Metadata
	acl, _ := json.Marshal(m.Acl)
	sseRequest, _ := json.Marshal(m.SseRequest)
	attrs, _ := json.Marshal(m.Attrs)
	sqltext := fmt.Sprintf("insert into multiparts values('%s','%s',%d,'%s','%s','%s','%s','%s','%s','%s','%s','%s')", multipart.BucketName, multipart.ObjectName, uploadtime, m.InitiatorId, m.OwnerId, m.ContentType, m.Location, m.Pool, acl, sseRequest, m.EncryptionKey, attrs)
	_, err = t.Client.Exec(sqltext)
	return
}

func (t *TidbClient) PutObjectPart(multipart Multipart, part Part) (err error) {
	uploadtime := uint64(multipart.InitialTime.UnixNano())
	lastt, err := time.Parse(CREATE_TIME_LAYOUT, part.LastModified)
	if err != nil {
		return
	}
	lastModified := lastt.Format(TIME_LAYOUT_TIDB)
	sqltext := fmt.Sprintf("insert into multipart values(%d,%d,'%s',%d,%s',%s',%s',%s',%s',%d,)", part.PartNumber, part.Size, part.ObjectId, part.Offset, part.Etag, lastModified, part.InitializationVector, multipart.BucketName, multipart.ObjectName, uploadtime)
	return
}

func (t *TidbClient) DeleteMultipart(multipart Multipart) (err error) { return }
func (t *TidbClient) ListMultipartUploads(bucketName, keyMarker, uploadIdMarker, prefix, delimiter, encodingType string, maxUploads int) (uploads []datatype.Upload, prefixs []string, isTruncated bool, nextKeyMarker, nextUploadIdMarker string, err error) {
	return
}
