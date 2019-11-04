package meta

import (
	"context"
	"database/sql"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/opentracing/opentracing-go"
)

func (m *Meta) GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error) {
	return m.Client.GetMultipart(bucketName, objectName, uploadId)
}

func (m *Meta) DeleteMultipart(multipart Multipart) (err error) {
	tx, err := m.Client.NewTrans()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			m.Client.AbortTrans(tx)
		}
	}()
	err = m.Client.DeleteMultipart(&multipart, tx)
	if err != nil {
		return
	}
	var removedSize int64 = 0
	for _, p := range multipart.Parts {
		removedSize += p.Size
	}
	err = m.Client.UpdateUsage(multipart.BucketName, -removedSize, tx)
	if err != nil {
		return
	}
	err = m.Client.CommitTrans(tx)
	return
}

func (m *Meta) PutObjectPart(multipart Multipart, part Part, ctx context.Context) (err error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "tidbPutObjectPart")
	defer span.Finish()
	tx, err := m.Client.NewTrans()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			m.Client.AbortTrans(tx)
		}
	}()
	err = m.Client.PutObjectPart(&multipart, &part, tx)
	if err != nil {
		return
	}
	var removedSize int64 = 0
	if part, ok := multipart.Parts[part.PartNumber]; ok {
		removedSize += part.Size
	}
	err = m.Client.UpdateUsage(multipart.BucketName, part.Size-removedSize, tx)
	if err != nil {
		return
	}
	err = m.Client.CommitTrans(tx)
	return
}

func (m *Meta) RenameObjectPart(object *Object, sourceObject string) (err error) {
	var tx *sql.Tx
	tx, err = m.Client.NewTrans()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			m.Client.AbortTrans(tx)
		}
	}()
	err = m.Client.RenameObjectPart(object, sourceObject, tx)
	if err != nil {
		return err
	}
	err = m.Client.RenameObject(object, sourceObject, tx)
	if err != nil {
		return err
	}
	err = m.Client.CommitTrans(tx)
	return err
}
