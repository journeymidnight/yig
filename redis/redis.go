package redis

import (
	"strconv"

	"github.com/journeymidnight/yig/helper"
)

var (
	redisClient *RedisCli
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
}

func Close() {
	if redisClient != nil && redisClient.IsValid() {
		err := redisClient.Close()
		if err != nil {
			helper.Logger.Printf(2, "Cannot close redis pool, err: %v", err)
		}
	}
}

func GetClient() (*RedisCli, error) {
	return redisClient, nil
}

func HasRedisClient() bool {
	if redisClient != nil && redisClient.IsValid() {
		return true
	}

	return false
}

func Remove(table RedisDatabase, prefix, key string) (err error) {
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return err
	}

	// Use table.String() + hashkey as Redis key
	_, err = c.Del(table.String() + prefix + helper.EscapeColon(key))
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis del for (%s), err: %v", table.String()+prefix+helper.EscapeColon(key), err)
		return err
	}
	helper.Logger.Printf(20, "Cmd: %s. Key: %s.", "DEL", table.String()+key)
	return nil
}

func Set(table RedisDatabase, prefix, key string, value interface{}) (err error) {
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return err
	}
	encodedValue, err := helper.MsgPackMarshal(value)
	if err != nil {
		helper.Logger.Printf(2, "failed to make pack for(%s, %s, %v), err: %v", prefix, key, value, err)
		return err
	}

	// Use table.String() + hashkey as Redis key. Set expire time to 30s.
	r, err := c.Set(table.String()+prefix+helper.EscapeColon(key), encodedValue, 30000)
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis Set(%s, %v), err: %v", table.String()+prefix+helper.EscapeColon(key), string(encodedValue), err)
		return err
	}
	helper.Logger.Printf(20, "Cmd: %s. Key: %s. Value: %s. Reply: %s.", "SET", table.String()+key, string(encodedValue), r)
	return nil
}

func Get(table RedisDatabase, prefix, key string) (value interface{}, err error) {
	var encodedValue []byte
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return nil, err
	}

	// Use table.String() + hashkey as Redis key
	encodedValue, err = c.Get(table.String() + prefix + helper.EscapeColon(key))
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis Get(%s), err: %v", table.String()+prefix+helper.EscapeColon(key), err)
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
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return nil, err
	}

	keys, err = c.Keys(query)
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis Keys(%s), err: %s", query, err)
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
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return nil, err
	}

	results, err = c.MGet(queryKeys)
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis MGet(%v), err: %v", queryKeys, err)
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

	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return "", err
	}

	result, err = c.MSet(tmpPairs)
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis MSet(%s, %v), err: %v", prefix, pairs, err)
		return "", err
	}

	return result, nil

}

func IncrBy(table RedisDatabase, prefix, key string, value int64) (int64, error) {
	var result int64
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return 0, err
	}

	result, err = c.IncrBy(prefix+helper.EscapeColon(key), value)
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis IncrBy(%s, %d), err: %v", prefix+helper.EscapeColon(key), value, err)
		return 0, err
	}
	return result, nil
}

// Get file bytes
// `start` and `end` are inclusive
// FIXME: this API causes an extra memory copy, need to patch radix to fix it
func GetBytes(key string, start int64, end int64) ([]byte, error) {
	var value []byte
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return nil, err
	}

	// Use table.String() + hashkey as Redis key
	value, err = c.GetRange(FileTable.String()+helper.EscapeColon(key), start, end)
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis GetRange(%s, %d, %d), err: %v", FileTable.String()+helper.EscapeColon(key), start, end, err)
		return nil, err
	}
	return value, nil
}

// Set file bytes
func SetBytes(key string, value []byte) (err error) {
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return err
	}

	// Use table.String() + hashkey as Redis key
	_, err = c.Set(FileTable.String()+helper.EscapeColon(key), value, 0)
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis set(%s), err: %d", FileTable.String()+helper.EscapeColon(key), err)
		return err
	}
	return nil
}

func HSet(table RedisDatabase, prefix, key, field string, value interface{}) (bool, error) {
	var r bool
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return false, err
	}

	r, err = c.HSet(table.String()+prefix+helper.EscapeColon(key), field, value)
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis HSet(%s, %s), err: %v", table.String()+prefix+helper.EscapeColon(key), field, err)
		return false, err
	}
	return r, nil
}

func HGet(table RedisDatabase, prefix, key, field string) (string, error) {
	var r string
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return "", err
	}

	r, err = c.HGet(table.String()+prefix+helper.EscapeColon(key), field)
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis HGet(%s, %s), err: %v", table.String()+prefix+helper.EscapeColon(key), field)
		return "", err
	}
	return r, nil
}

func HDel(table RedisDatabase, prefix, key string, fields []string) (int64, error) {
	var r int64
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return 0, err
	}

	r, err = c.HDel(table.String()+prefix+helper.EscapeColon(key), fields)
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis HDel(%s, %v), err: %v", table.String()+prefix+helper.EscapeColon(key), fields, err)
		return 0, err
	}
	return r, nil
}

func HGetInt64(table RedisDatabase, prefix, key, field string) (int64, error) {
	var r int64
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return 0, err
	}

	theKey := table.String() + prefix + helper.EscapeColon(key)
	r, err = c.HGetInt64(theKey, field)
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis HGetInt64(%s, %s), err: %v", theKey, field, err)
		return 0, err
	}
	return r, nil
}

func HGetAll(table RedisDatabase, prefix, key string) (map[string]string, error) {
	var r map[string]string
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return nil, err
	}

	r, err = c.HGetAll(table.String() + prefix + helper.EscapeColon(key))
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis HGetAll(%s), err: %v", table.String()+prefix+helper.EscapeColon(key), err)
		return nil, err
	}
	return r, nil
}

func HIncrBy(table RedisDatabase, prefix, key, field string, incr int64) (int64, error) {
	var r int64
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return 0, err
	}

	r, err = c.HIncrBy(table.String()+prefix+helper.EscapeColon(key), field, incr)
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis HIncrBy(%s, %s, %d), err: %v", table.String()+prefix+helper.EscapeColon(key), field, incr)
		return 0, err
	}
	return r, nil
}

func HMSet(table RedisDatabase, prefix, key string, fields map[string]interface{}) (string, error) {
	var r string
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return "", err
	}

	r, err = c.HMSet(table.String()+prefix+helper.EscapeColon(key), fields)
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis HMSet(%s, %v), err: %v", table.String()+prefix+helper.EscapeColon(key), fields, err)
		return "", err
	}
	return r, nil
}

func HMGet(table RedisDatabase, prefix, key string, fields []string) (map[string]interface{}, error) {
	var r map[string]interface{}
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return nil, err
	}

	r, err = c.HMGet(table.String()+prefix+helper.EscapeColon(key), fields)
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis HMGet(%s, %v), err: %v", table.String()+prefix+helper.EscapeColon(key), fields, err)
		return nil, err
	}
	return r, nil
}

// Publish the invalid message to other YIG instances through Redis
func Invalid(table RedisDatabase, key string) (err error) {
	c, err := GetClient()
	if err != nil {
		helper.Logger.Printf(2, "failed to get redis client, err: %v", err)
		return err
	}

	// Use table.String() + hashkey as Redis key
	_, err = c.Publish(table.InvalidQueue(), helper.EscapeColon(key))
	if err != nil {
		helper.Logger.Printf(2, "failed to call redis Public(%s), err: %v", helper.EscapeColon(key), err)
		return err
	}
	return nil
}
