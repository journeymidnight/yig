package redis

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/cep21/circuit"
	"github.com/journeymidnight/yig/circuitbreak"
	"github.com/journeymidnight/yig/helper"
	"github.com/minio/highwayhash"
)

var (
	redisClient  *RedisCli
	CacheCircuit *circuit.Circuit
)

const InvalidQueueName = "InvalidQueue"

const keyvalue = "000102030405060708090A0B0C0D0E0FF0E0D0C0B0A090807060504030201000" // This is the key for hash sum !

type RedisDatabase int

func (r RedisDatabase) String() string {
	return strconv.Itoa(int(r))
}

func (r RedisDatabase) InvalidQueue() string {
	return InvalidQueueName + r.String()
}

const (
	UserTable RedisDatabase = iota
	BucketTable
	ObjectTable
	FileTable
	ClusterTable
)

var MetadataTables = []RedisDatabase{UserTable, BucketTable, ObjectTable, ClusterTable}
var DataTables = []RedisDatabase{FileTable}

func Initialize() {
	redisClient = NewRedisCli()
	redisClient.Init()
	CacheCircuit = circuitbreak.NewCacheCircuit()
}

func Close() {
	err := redisClient.Close()
	if err != nil {
		helper.ErrorIf(err, "Cannot close redis pool.")
	}
}

func GetClient(ctx context.Context) (*RedisCli, error) {
	return redisClient, nil
}

func HasRedisClient() error {
	if redisClient != nil && redisClient.IsValid() {
		return nil
	}
	return errors.New("there is no valid redis client yet.")
}

func Remove(table RedisDatabase, key string) (err error) {
	return CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			_, err = c.Del(table.String() + hashkey)
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s. Key: %s.", "DEL", table.String()+key)
			return err
		},
		nil,
	)
}

func Set(table RedisDatabase, key string, value interface{}) (err error) {
	return CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}
			encodedValue, err := helper.MsgPackMarshal(value)
			if err != nil {
				return err
			}
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key. Set expire time to 30s.
			r, err := c.Set(table.String()+hashkey, string(encodedValue), 30000)
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s. Key: %s. Value: %s. Reply: %s.", "SET", table.String()+key, string(encodedValue), r)
			return err
		},
		nil,
	)

}

func Get(table RedisDatabase, key string,
	unmarshal func([]byte) (interface{}, error)) (value interface{}, err error) {
	var encodedValue []byte
	err = CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			encodedValue, err = c.Get(table.String() + hashkey)
			if err != nil {
				return err
			}
			return nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	if len(encodedValue) == 0 {
		return nil, nil
	}
	return unmarshal(encodedValue)
}

// Get file bytes
// `start` and `end` are inclusive
// FIXME: this API causes an extra memory copy, need to patch radix to fix it
func GetBytes(key string, start int64, end int64) ([]byte, error) {
	var value []byte
	err := CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			value, err = c.GetRange(FileTable.String()+hashkey, start, end)
			if err != nil {
				return err
			}
			return nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Set file bytes
func SetBytes(key string, value []byte) (err error) {
	return CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			r, err := c.Set(FileTable.String()+hashkey, value, 0)
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s. Key: %s. Value: %s. Reply: %s.", "SET", FileTable.String()+key, string(value), r)
			return err
		},
		nil,
	)
}

// Publish the invalid message to other YIG instances through Redis
func Invalid(table RedisDatabase, key string) (err error) {
	return CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			r, err := c.Publish(table.InvalidQueue(), hashkey)
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s. Queue: %s. Key: %s. Reply: %d.", "PUBLISH", table.InvalidQueue(), FileTable.String()+key, r)
			return err
		},
		nil,
	)

}

// Get Object to HighWayHash for redis
func HashSum(ObjectName string) (string, error) {
	key, err := hex.DecodeString(keyvalue)
	if err != nil {
		return "", err
	}

	ObjectNameString := strings.NewReader(ObjectName)

	hash, err := highwayhash.New(key)
	if err != nil {
		return "", err
	}

	if _, err = io.Copy(hash, ObjectNameString); err != nil {
		return "", err
	}

	sumresult := hex.EncodeToString(hash.Sum(nil))

	return sumresult, nil
}
