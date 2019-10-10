package redis

import (
	"encoding/hex"
	"fmt"
	"github.com/minio/highwayhash"
	"io"
	"strconv"
	"strings"

	"context"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/journeymidnight/yig/circuitbreak"
	"github.com/journeymidnight/yig/helper"
	"time"
	"github.com/cep21/circuit"
)

var (
	redisPool *redigo.Pool
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
	UserTable    RedisDatabase = iota
	BucketTable  
	ObjectTable  
	FileTable    
	ClusterTable 
)

var MetadataTables = []RedisDatabase{UserTable, BucketTable, ObjectTable, ClusterTable}
var DataTables = []RedisDatabase{FileTable}

func Initialize() {

	options := []redigo.DialOption{
		redigo.DialReadTimeout(time.Duration(helper.CONFIG.RedisReadTimeout) * time.Second),
		redigo.DialConnectTimeout(time.Duration(helper.CONFIG.RedisConnectTimeout) * time.Second),
		redigo.DialWriteTimeout(time.Duration(helper.CONFIG.RedisWriteTimeout) * time.Second),
		redigo.DialKeepAlive(time.Duration(helper.CONFIG.RedisKeepAlive) * time.Second),
	}

	if helper.CONFIG.RedisPassword != "" {
		options = append(options, redigo.DialPassword(helper.CONFIG.RedisPassword))
	}

	df := func() (redigo.Conn, error) {
		c, err := redigo.Dial("tcp", helper.CONFIG.RedisAddress, options...)
		if err != nil {
			return nil, err
		}
		return c, nil
	}

	CacheCircuit = circuitbreak.NewCacheCircuit()
	redisPool = &redigo.Pool{
			MaxIdle:     helper.CONFIG.RedisPoolMaxIdle,
			IdleTimeout: time.Duration(helper.CONFIG.RedisPoolIdleTimeout) * time.Second,
			// Other pool configuration not shown in this example.
			Dial: df,
	}
}

func Pool() *redigo.Pool {
	return redisPool
}

func Close() {
	err := redisPool.Close()
	if err != nil {
		helper.Logger.Error("Cannot close redis pool:", err)
	}
}

func GetClient(ctx context.Context) (redigo.Conn, error) {
	return redisPool.GetContext(ctx)
}

func Remove(table RedisDatabase, key string) (err error) {
	return CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}
			defer c.Close()
			hashkey,err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			_, err = c.Do("DEL", table.String()+hashkey)
			if err == redigo.ErrNil {
				return nil
			}
			if err != nil {
				helper.Logger.Error("Redis DEL", table.String()+key,
					"error:", err)
			}
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
			defer c.Close()
			encodedValue, err := helper.MsgPackMarshal(value)
			if err != nil {
				return err
			}
			hashkey,err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key. Set expire time to 30s.
			r, err := redigo.String(c.Do("SET", table.String()+hashkey, string(encodedValue), "EX", 30))
			if err == redigo.ErrNil {
				return nil
			}
			if err != nil {
				helper.Logger.Error(
					fmt.Sprintf("Cmd: SET. Key: %s. Value: %s. Reply: %s.",
						table.String()+key, string(encodedValue), r))
			}
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
			defer c.Close()
			hashkey,err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			encodedValue, err = redigo.Bytes(c.Do("GET", table.String()+hashkey))
			if err != nil {
				if err == redigo.ErrNil {
					return nil
				}
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
			defer c.Close()
			hashkey,err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			value, err = redigo.Bytes(c.Do("GETRANGE", FileTable.String()+hashkey, start, end))
			if err != nil {
				if err == redigo.ErrNil {
					return nil
				}
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
			defer c.Close()
			hashkey,err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			r, err := redigo.String(c.Do("SET", FileTable.String()+hashkey, value))
			if err == redigo.ErrNil {
				return nil
			}
			if err != nil {
				helper.Logger.Error(fmt.Sprintf("Cmd: SET. Key: %s. Value: %s. Reply: %s.",
					FileTable.String()+key, string(value), r))
			}
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
			defer c.Close()
			hashkey,err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			r, err := redigo.String(c.Do("PUBLISH", table.InvalidQueue(), hashkey))
			if err == redigo.ErrNil {
				return nil
			}
			if err != nil {
				helper.Logger.Error(fmt.Sprintf("Cmd: PUBLISH. Queue: %s. Key: %s. Reply: %s.",
					table.InvalidQueue(), FileTable.String()+key, r))
			}
			return err
		},
		nil,
	)
}

// Get Object to HighWayHash for redis
func HashSum(ObjectName string) (string,error) {
	key, err := hex.DecodeString(keyvalue)
	if err != nil {
		return "",err
	}

	ObjectNameString := strings.NewReader(ObjectName)

	hash, err := highwayhash.New(key)
	if err != nil {
		return "",err
	}

	if _, err = io.Copy(hash, ObjectNameString); err != nil {
		return "",err
	}

	sumresult := hex.EncodeToString(hash.Sum(nil))

	return sumresult,nil
}