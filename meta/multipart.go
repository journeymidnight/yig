package meta

import (
	. "github.com/journeymidnight/yig/meta/types"
)

func (m *Meta) GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error) {
	return m.Client.GetMultipart(bucketName, objectName, uploadId)
}

func (m *Meta) DeleteMultipart(multipart Multipart) (removedSize int64, err error) {
	tx, err := m.Client.NewTrans()
	if err != nil {
		return
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
	for _, p := range multipart.Parts {
		removedSize += p.Size
	}
	err = m.Client.UpdateUsage(multipart.BucketName, -removedSize, tx)
	if err != nil {
		return
	}
	err = m.Client.CommitTrans(tx)
	return -removedSize, nil
}

func (m *Meta) PutObjectPart(multipart Multipart, part Part) (deltaSize int64, err error) {
	return m.Client.PutObjectPart(&multipart, &part)
}
