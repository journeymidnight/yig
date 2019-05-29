package redis

import (
	"context"
	"strconv"

	"github.com/cep21/circuit"
	"github.com/journeymidnight/yig/circuitbreak"
	"github.com/journeymidnight/yig/helper"
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

func HasRedisClient() bool {
	if redisClient != nil && redisClient.IsValid() {
		return true
	}

	return false
}

func Remove(table RedisDatabase, prefix, key string) (err error) {
	return CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}

			// Use table.String() + hashkey as Redis key
			_, err = c.Del(table.String() + prefix + helper.EscapeColon(key))
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s. Key: %s.", "DEL", table.String()+key)
			return err
		},
		nil,
	)
}

func Set(table RedisDatabase, prefix, key string, value interface{}) (err error) {
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

			// Use table.String() + hashkey as Redis key. Set expire time to 30s.
			r, err := c.Set(table.String()+prefix+helper.EscapeColon(key), encodedValue, 30000)
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s. Key: %s. Value: %s. Reply: %s.", "SET", table.String()+key, string(encodedValue), r)
			return err
		},
		nil,
	)

}

func Get(table RedisDatabase, prefix, key string) (value interface{}, err error) {
	var encodedValue []byte
	err = CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}

			// Use table.String() + hashkey as Redis key
			encodedValue, err = c.Get(table.String() + prefix + helper.EscapeColon(key))
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
	err = helper.MsgPackUnMarshal(encodedValue, value)
	return value, err
}

// don't use the escapecolon in keys command.
func Keys(table RedisDatabase, pattern string) ([]string, error) {
	var keys []string
	query := table.String() + pattern
	err := CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}

			keys, err = c.Keys(query)
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

	return keys, nil

}

func MGet(table RedisDatabase, prefix string, keys []string) ([]interface{}, error) {
	var results []interface{}
	var queryKeys []string
	for _, key := range keys {
		queryKeys = append(queryKeys, table.String()+prefix+helper.EscapeColon(key))
	}
	err := CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}

			results, err = c.MGet(queryKeys)
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

	return results, nil
}

func MSet(table RedisDatabase, prefix string, pairs map[string]interface{}) (string, error) {
	var result string
	tmpPairs := make(map[interface{}]interface{})
	for k, v := range pairs {
		tmpPairs[table.String()+prefix+helper.EscapeColon(k)] = v
	}

	err := CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}

			result, err = c.MSet(tmpPairs)
			if err != nil {
				return err
			}
			return nil
		},
		nil,
	)

	if err != nil {
		return "", err
	}

	return result, nil

}

func IncrBy(table RedisDatabase, prefix, key string, value int64) (int64, error) {
	var result int64
	err := CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}

			result, err = c.IncrBy(prefix+helper.EscapeColon(key), value)
			if err != nil {
				return err
			}
			return nil
		},
		nil,
	)
	if err != nil {
		helper.Logger.Println(2, "failed to call IncrBy with key: ", key, ", value: ", value)
		return 0, err
	}
	return result, err
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

			// Use table.String() + hashkey as Redis key
			value, err = c.GetRange(FileTable.String()+helper.EscapeColon(key), start, end)
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

			// Use table.String() + hashkey as Redis key
			r, err := c.Set(FileTable.String()+helper.EscapeColon(key), value, 0)
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s. Key: %s. Value: %s. Reply: %s.", "SET", FileTable.String()+key, string(value), r)
			return err
		},
		nil,
	)
}

func HSet(table RedisDatabase, prefix, key, field string, value interface{}) (bool, error) {
	var r bool
	err := CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}

			r, err = c.HSet(table.String()+prefix+helper.EscapeColon(key), field, value)
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s, key: %s, field: %s, value %v", "HSet", table.String()+helper.EscapeColon(key), field, value)
			return err
		},
		nil,
	)

	if err != nil {
		return false, err
	}
	return r, nil
}

func HGet(table RedisDatabase, prefix, key, field string) (string, error) {
	var r string
	err := CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}

			r, err = c.HGet(table.String()+prefix+helper.EscapeColon(key), field)
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s, key: %s, field: %s", "HGet", table.String()+helper.EscapeColon(key), field)
			return err
		},
		nil,
	)

	if err != nil {
		return "", err
	}
	return r, nil
}

func HGetInt64(table RedisDatabase, prefix, key, field string) (int64, error) {
	var r int64
	err := CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}

			theKey := table.String() + prefix + helper.EscapeColon(key)
			r, err = c.HGetInt64(theKey, field)
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s, key: %s, field: %s", "HGetInt64", theKey, field)
			return err
		},
		nil,
	)

	if err != nil {
		return 0, err
	}
	return r, nil
}

func HGetAll(table RedisDatabase, prefix, key string) (map[string]string, error) {
	var r map[string]string
	err := CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}

			r, err = c.HGetAll(table.String() + prefix + helper.EscapeColon(key))
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s, key: %s", "HGetAll", table.String()+helper.EscapeColon(key))
			return err
		},
		nil,
	)

	if err != nil {
		return nil, err
	}
	return r, nil
}

func HIncrBy(table RedisDatabase, prefix, key, field string, incr int64) (int64, error) {
	var r int64
	err := CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}

			r, err = c.HIncrBy(table.String()+prefix+helper.EscapeColon(key), field, incr)
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s, key: %s, field: %s, incr: %d", "HIncrBy", table.String()+helper.EscapeColon(key), field, incr)
			return err
		},
		nil,
	)

	if err != nil {
		return 0, err
	}
	return r, nil
}

func HMSet(table RedisDatabase, prefix, key string, fields map[string]interface{}) (string, error) {
	var r string
	err := CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}

			r, err = c.HMSet(table.String()+prefix+helper.EscapeColon(key), fields)
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s, key: %s, field: %v", "HMSet", table.String()+helper.EscapeColon(key), fields)
			return err
		},
		nil,
	)

	if err != nil {
		return "", err
	}
	return r, nil
}

func HMGet(table RedisDatabase, prefix, key string, fields []string) (map[string]interface{}, error) {
	var r map[string]interface{}
	err := CacheCircuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			c, err := GetClient(ctx)
			if err != nil {
				return err
			}

			r, err = c.HMGet(table.String()+prefix+helper.EscapeColon(key), fields)
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s, key: %s, field: %v", "HMGet", table.String()+helper.EscapeColon(key), fields)
			return err
		},
		nil,
	)

	if err != nil {
		return nil, err
	}
	return r, nil
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

			// Use table.String() + hashkey as Redis key
			r, err := c.Publish(table.InvalidQueue(), helper.EscapeColon(key))
			if err == nil {
				return nil
			}
			helper.ErrorIf(err, "Cmd: %s. Queue: %s. Key: %s. Reply: %d.", "PUBLISH", table.InvalidQueue(), FileTable.String()+key, r)
			return err
		},
		nil,
	)

}
