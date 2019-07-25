package meta

import (
	"context"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/redis"
)

const (
	BUCKET_NUMBER_LIMIT = 100
)

func (m *Meta) GetUserBuckets(userId string, willNeed bool, ctx context.Context) (buckets []string, err error) {
	getUserBuckets := func() (bs interface{}, err error) {
		return m.Client.GetUserBuckets(userId)
	}
	unmarshaller := func(in []byte) (interface{}, error) {
		buckets := make([]string, 0)
		err := helper.MsgPackUnMarshal(in, &buckets)
		return buckets, err
	}
	bs, err := m.Cache.Get(redis.UserTable, userId, getUserBuckets, unmarshaller, willNeed, ctx)
	if err != nil {
		return
	}
	buckets, ok := bs.([]string)
	if !ok {
		helper.Debugln("[", helper.RequestIdFromContext(ctx), "]", "Cast bs failed:", bs)
		err = ErrInternalError
		return
	}
	return buckets, nil
}

func (m *Meta) AddBucketForUser(bucketName string, userId string, ctx context.Context) (err error) {
	buckets, err := m.GetUserBuckets(userId, false, ctx)
	if err != nil {
		return err
	}
	if len(buckets)+1 > BUCKET_NUMBER_LIMIT {
		return ErrTooManyBuckets
	}
	return m.Client.AddBucketForUser(bucketName, userId)
}

func (m *Meta) RemoveBucketForUser(bucketName string, userId string) (err error) {
	return m.Client.RemoveBucketForUser(bucketName, userId)
}
