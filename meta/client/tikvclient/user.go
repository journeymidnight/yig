package tikvclient

import (
	"errors"
	"strings"
)

func genUserBucketKey(ownerId, bucketName string) []byte {
	return GenKey(TableUserBucketPrefix, ownerId, bucketName)
}

const (
	MaxUserBucketKey = 100
)

//user
// Key: u\{OwnerId}\{BucketName}
func (c *TiKVClient) GetUserBuckets(userId string) (buckets []string, err error) {
	startKey := genUserBucketKey(userId, TableMinKeySuffix)
	endKey := genUserBucketKey(userId, TableMaxKeySuffix)
	kvs, err := c.TxScan(startKey, endKey, MaxUserBucketKey)
	if err != nil {
		return nil, err
	}
	for _, kv := range kvs {
		k := strings.Split(string(kv.K), TableSeparator)
		if len(k) != 3 {
			return nil, errors.New("Invalid user bucket key:" + string(kv.K))
		}
		buckets = append(buckets, k[2])
	}
	return
}
