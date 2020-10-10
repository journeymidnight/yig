package tikvclient

import (
	"errors"
	"math"
	"strings"

	. "github.com/journeymidnight/yig/error"
)

func GenUserBucketKey(ownerId, bucketName string) []byte {
	return GenKey(TableUserBucketPrefix, ownerId, bucketName)
}

const (
	MaxUserBucketKey = 100
)

//user
// Key: u\{OwnerId}\{BucketName}
func (c *TiKVClient) GetUserBuckets(userId string) (buckets []string, err error) {
	startKey := GenUserBucketKey(userId, TableMinKeySuffix)
	endKey := GenUserBucketKey(userId, TableMaxKeySuffix)
	kvs, err := c.TxScan(startKey, endKey, MaxUserBucketKey, nil)
	if err != nil {
		return nil, NewError(InTikvFatalError, "GetUserBuckets TxScan err", err)
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
	bucketUser = make(map[string]string)
	startKey := GenKey(TableUserBucketPrefix, TableMinKeySuffix)
	endKey := GenKey(TableUserBucketPrefix, TableMaxKeySuffix)
	kvs, err := c.TxScan(startKey, endKey, math.MaxInt64, nil)
	for _, kv := range kvs {
		k := strings.Split(string(kv.K), TableSeparator)
		if len(k) != 3 {
			return nil, NewError(InTikvFatalError, "Invalid user bucket key:"+string(kv.K), nil)
		}
		bucketUser[k[2]] = k[1]
	}
	return bucketUser, nil
}
