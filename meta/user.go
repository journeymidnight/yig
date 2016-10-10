package meta

import (
	"encoding/json"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/redis"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
)

const (
	BUCKET_NUMBER_LIMIT = 100
)

func (m *Meta) GetUserBuckets(userId string) (buckets []string, err error) {
	getUserBuckets := func() (bs interface{}, err error) {
		getRequest, err := hrpc.NewGetStr(context.Background(), USER_TABLE, userId)
		if err != nil {
			return
		}
		response, err := m.Hbase.Get(getRequest)
		if err != nil {
			m.Logger.Println("Error getting user info, with error ", err)
			return
		}
		buckets := make([]string, 0, len(response.Cells))
		for _, cell := range response.Cells {
			buckets = append(buckets, string(cell.Qualifier))
		}
		return buckets, nil
	}
	unmarshaller := func(in []byte) (interface{}, error) {
		buckets := make([]string, 0)
		err := json.Unmarshal(in, &buckets)
		return buckets, err
	}
	bs, err := m.Cache.Get(redis.UserTable, userId, getUserBuckets, unmarshaller)
	if err != nil {
		return
	}
	buckets, ok := bs.([]string)
	if !ok {
		helper.Debugln("Cast bs failed:", bs)
		err = ErrInternalError
		return
	}
	return buckets, nil
}

func (m *Meta) AddBucketForUser(bucketName string, userId string) (err error) {
	buckets, err := m.GetUserBuckets(userId)
	if err != nil {
		return err
	}
	if len(buckets)+1 > BUCKET_NUMBER_LIMIT {
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
