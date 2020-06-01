package tikvclient

import (
	"errors"
	"math"
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
	kvs, err := c.TxScan(startKey, endKey, MaxUserBucketKey, nil)
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

func (c *TiKVClient) GetAllUserBuckets() (bucketUser map[string]string, err error) {
	startKey := GenKey(TableUserBucketPrefix, TableMinKeySuffix)
	endKey := GenKey(TableUserBucketPrefix, TableMaxKeySuffix)
	kvs, err := c.TxScan(startKey, endKey, math.MaxInt64, nil)
	for _, kv := range kvs {
		k := strings.Split(string(kv.K), TableSeparator)
		if len(k) != 3 {
			return nil, errors.New("Invalid user bucket key:" + string(kv.K))
		}
		bucketUser[k[2]] = k[1]
	}
	return bucketUser, nil
}
