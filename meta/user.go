package meta

import (
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
	. "git.letv.cn/yig/yig/error"
	"github.com/tsuna/gohbase/filter"
)

const (
	BUCKET_NUMBER_LIMIT = 100
)

func (m *Meta) GetUserBuckets(userId string) (buckets []string, err error) {
	getRequest, err := hrpc.NewGetStr(context.Background(), USER_TABLE, userId)
	if err != nil {
		return
	}
	response, err := m.Hbase.Get(getRequest)
	if err != nil {
		m.Logger.Println("Error getting user info, with error ", err)
		return
	}
	for _, cell := range response.Cells {
		buckets = append(buckets, string(cell.Qualifier))
	}
	return buckets, nil
}

func (m *Meta) AddBucketForUser(bucketName string, userId string) (err error) {
	buckets, err := m.GetUserBuckets(userId)
	if err != nil {
		return err
	}
	if len(buckets) > BUCKET_NUMBER_LIMIT {
		return ErrTooManyBuckets
	}

	newUserBucket := map[string]map[string][]byte{
		USER_COLUMN_FAMILY: map[string][]byte{
			bucketName: []byte{},
		},
	}
	putRequest, err := hrpc.NewPutStr(context.Background(), USER_TABLE, userId, newUserBucket)
	if err != nil {
		return err
	}
	_, err = m.Hbase.Put(putRequest)
	return
}

func (m *Meta) RemoveBucketForUser(bucketName string, userId string) (err error) {
	deleteValue := map[string]map[string][]byte{
		USER_COLUMN_FAMILY: map[string][]byte{
			bucketName: []byte{},
		},
	}
	deleteRequest, err := hrpc.NewDelStr(context.Background(), USER_TABLE, userId, deleteValue)
	if err != nil {
		return
	}
	_, err = m.Hbase.Delete(deleteRequest)
	return
}
