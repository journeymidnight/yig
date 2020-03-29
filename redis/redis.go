package redis

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/cep21/circuit"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/journeymidnight/yig/circuitbreak"
	"github.com/journeymidnight/yig/helper"
	"github.com/minio/highwayhash"
)

var (
	redisPoolHR *HashRing
	redisPools  []*redigo.Pool
	Circuits    []*circuit.Circuit
)

const InvalidQueueName = "InvalidQueue"

const keyvalue = "000102030405060708090A0B0C0D0E0FF0E0D0C0B0A090807060504030201000" // This is the key for hash sum !

const hashReplicationCount = 4096

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

	options := []redigo.DialOption{
		redigo.DialReadTimeout(time.Duration(helper.CONFIG.RedisReadTimeout) * time.Second),
		redigo.DialConnectTimeout(time.Duration(helper.CONFIG.RedisConnectTimeout) * time.Second),
		redigo.DialWriteTimeout(time.Duration(helper.CONFIG.RedisWriteTimeout) * time.Second),
		redigo.DialKeepAlive(time.Duration(helper.CONFIG.RedisKeepAlive) * time.Second),
	}

	if helper.CONFIG.RedisPassword != "" {
		options = append(options, redigo.DialPassword(helper.CONFIG.RedisPassword))
	}
	key, err := hex.DecodeString(keyvalue)
	if err != nil {
		helper.Logger.Println("Get redis hash err:", err)
		panic(err)
	}
	hash, err := highwayhash.New64(key)
	if err != nil {
		helper.Logger.Println("Get redis hash err:", err)
		panic(err)
	}
	redisPoolHR = NewHashRing(hashReplicationCount, hash)
	for i, addr := range helper.CONFIG.RedisGroup {
		initPool(i, addr, options...)
	}
}

func initPool(i int, addr string, options ...redigo.DialOption) {
	df := func() (redigo.Conn, error) {
		c, err := redigo.Dial("tcp", addr, options...)
		if err != nil {
			return nil, err
		}
		return c, nil
	}

	helper.Logger.Info("RedisGroup ADDR:", addr)
	cb := circuitbreak.NewCacheCircuit()
	pool := &redigo.Pool{
		MaxIdle:     helper.CONFIG.RedisPoolMaxIdle,
		IdleTimeout: time.Duration(helper.CONFIG.RedisPoolIdleTimeout) * time.Second,
		// Other pool configuration not shown in this example.
		Dial: df,
	}
	Circuits = append(Circuits, cb)
	redisPools = append(redisPools, pool)
	err := redisPoolHR.Add(i)
	if err != nil {
		panic(err)
	}
}

func Close(i int) {
	if i > len(helper.CONFIG.RedisGroup) {
		return
	}
	err := redisPools[i].Close()
	if err != nil {
		helper.Logger.Error("Cannot close redis pool:", err)
	}
}

func CloseAll() {
	for i := 0; i < len(helper.CONFIG.RedisGroup); i++ {
		err := redisPools[i].Close()
		if err != nil {
			helper.Logger.Error("Cannot close redis pool:", err)
		}
	}
}

func GetLocate(key string) (int, error) {
	n, err := redisPoolHR.Locate(key)
	if err != nil {
		return 0, err
	}
	return n.(int), nil
}

func GetClient(ctx context.Context, i int) (redigo.Conn, error) {
	return redisPools[i].GetContext(ctx)
}

func Remove(table RedisDatabase, key string) (err error) {
	i, err := GetLocate(key)
	if err != nil {
		return
	}
	return Circuits[i].Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx, i)
			if err != nil {
				return err
			}
			defer c.Close()
			hashkey, err := HashSum(key)
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
	i, err := GetLocate(key)
	if err != nil {
		return
	}
	return Circuits[i].Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx, i)
			if err != nil {
				return err
			}
			defer c.Close()
			encodedValue, err := helper.MsgPackMarshal(value)
			if err != nil {
				return err
			}
			hashkey, err := HashSum(key)
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
	i, err := GetLocate(key)
	if err != nil {
		return
	}
	var encodedValue []byte
	err = Circuits[i].Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx, i)
			if err != nil {
				return err
			}
			defer c.Close()
			hashkey, err := HashSum(key)
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

// Get Usages
// `start` and `end` are inclusive
func GetUsage(key string) (value string, err error) {
	var encodedValue string
	for circuitNum, circuitTarget := range Circuits {
		err = circuitTarget.Execute(
			context.Background(),
			func(ctx context.Context) (err error) {
				c, err := GetClient(ctx, circuitNum)
				if err != nil {
					return err
				}
				defer c.Close()
				// Use table.String() + hashkey as Redis key
				encodedValue, err = redigo.String(c.Do("GET", key))
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
			return "", err
		}
		if len(encodedValue) == 0 {
			continue
		}
		return encodedValue, nil
	}
	return "", nil
}

// Get file bytes
// `start` and `end` are inclusive
// FIXME: this API causes an extra memory copy, need to patch radix to fix it
func GetBytes(key string, start int64, end int64) ([]byte, error) {
	i, err := GetLocate(key)
	if err != nil {
		return nil, err
	}
	var value []byte
	err = Circuits[i].Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx, i)
			if err != nil {
				return err
			}
			defer c.Close()
			hashkey, err := HashSum(key)
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
	i, err := GetLocate(key)
	if err != nil {
		return
	}
	return Circuits[i].Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx, i)
			if err != nil {
				return err
			}
			defer c.Close()
			hashkey, err := HashSum(key)
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
	i, err := GetLocate(key)
	if err != nil {
		return
	}
	return Circuits[i].Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx, i)
			if err != nil {
				return err
			}
			defer c.Close()
			hashkey, err := HashSum(key)
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
