package hbaseclient

import (
	"context"
	"github.com/cannium/gohbase/hrpc"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

func (h *HbaseClient) GetUserBuckets(userId string) (buckets []string, err error) {
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	getRequest, err := hrpc.NewGetStr(ctx, USER_TABLE, userId)
	if err != nil {
		return
	}
	response, err := h.Client.Get(getRequest)
	if err != nil {
		return
	}
	buckets = make([]string, 0, len(response.Cells))
	for _, cell := range response.Cells {
		buckets = append(buckets, string(cell.Qualifier))
	}
	return buckets, nil
}

func (h *HbaseClient) AddBucketForUser(bucketName, userId string) (err error) {
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
	_, err = h.Client.Put(putRequest)
	return
}

func (h *HbaseClient) RemoveBucketForUser(bucketName string, userId string) (err error) {
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
	_, err = h.Client.Delete(deleteRequest)
	return
}
