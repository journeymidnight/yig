package meta

import (
	"errors"
	"fmt"
	"context"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
)

const (
	BUCKET_NUMBER_LIMIT = 100
)

type BucketNameList struct {
	BucketNames []string
}

func (bnl *BucketNameList) Serialize() (map[string]interface{}, error) {
	fields := make(map[string]interface{})
	body, err := helper.MsgPackMarshal(bnl)
	if err != nil {
		return nil, err
	}
	fields[types.FIELD_NAME_BODY] = string(body)
	return fields, nil
}

func (bnl *BucketNameList) Deserialize(fields map[string]string) (interface{}, error) {
	body, ok := fields[types.FIELD_NAME_BODY]
	if !ok {
		return nil, errors.New(fmt.Sprintf("no field %s found", types.FIELD_NAME_BODY))
	}

	val := &BucketNameList{}
	err := helper.MsgPackUnMarshal([]byte(body), val)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("got invalid body %s for BucketNameList, err: %v", body, err))
	}
	return val, nil
}

func (m *Meta) GetUserBuckets(ctx context.Context, userId string, willNeed bool) (buckets []string, err error) {
	getUserBuckets := func() (helper.Serializable, error) {
		bucketStrs, err := m.Client.GetUserBuckets(userId)
		if err != nil {
			return nil, err
		}
		bnl := &BucketNameList{
			BucketNames: bucketStrs,
		}
		return bnl, nil
	}

	toBucket := func(fields map[string]string) (interface{}, error) {
		b := &BucketNameList{}
		return b.Deserialize(fields)
	}

	bs, err := m.Cache.Get(ctx, redis.UserTable, BUCKET_CACHE_PREFIX, userId, getUserBuckets, toBucket, willNeed)
	if err != nil {
		return
	}

	bnl, ok := bs.(*BucketNameList)
	if !ok {
		helper.Debugln("[", helper.RequestIdFromContext(ctx), "]", "Cast bs failed:", bs)
		err = ErrInternalError
		return
	}
	buckets = bnl.BucketNames
	return buckets, nil
}

func (m *Meta) AddBucketForUser(ctx context.Context, bucketName string, userId string) (err error) {
	buckets, err := m.GetUserBuckets(ctx, userId, false)
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
