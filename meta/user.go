package meta

import (
	"context"

	"github.com/cannium/gohbase/hrpc"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/redis"
)

const (
	BUCKET_NUMBER_LIMIT = 100
)

func (m *Meta) GetUserBuckets(userId string, willNeed bool) (buckets []string, err error) {
	getUserBuckets := func() (bs interface{}, err error) {
		ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
		defer done()
		getRequest, err := hrpc.NewGetStr(ctx, USER_TABLE, userId)
		if err != nil {
			return
		}
		response, err := m.Hbase.Get(getRequest)
		if err != nil {
			m.Logger.Println(5, "Error getting user info, with error ", err)
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
		err := helper.MsgPackUnMarshal(in, &buckets)
		return buckets, err
	}
	bs, err := m.Cache.Get(redis.UserTable, userId, getUserBuckets, unmarshaller, willNeed)
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
	buckets, err := m.GetUserBuckets(userId, false)
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
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	putRequest, err := hrpc.NewPutStr(ctx, USER_TABLE, userId, newUserBucket)
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
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	deleteRequest, err := hrpc.NewDelStr(ctx, USER_TABLE, userId, deleteValue)
	if err != nil {
		return
	}
	_, err = m.Hbase.Delete(deleteRequest)
	return
}
