package redis

import (
	"encoding/json"
	"legitlab.letv.cn/yig/yig/helper"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
	"strconv"
)

const InvalidQueueName = "InvalidQueue"

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

func TableFromChannelName(name string) (r RedisDatabase, err error) {
	tableString := name[len(InvalidQueueName):]
	tableNumber, err := strconv.Atoi(tableString)
	if err != nil {
		return
	}
	r = RedisDatabase(tableNumber)
	return
}

var MetadataTables = []RedisDatabase{UserTable, BucketTable, ObjectTable, ClusterTable}
var DataTables = []RedisDatabase{FileTable}

var redisConnectionPool *pool.Pool

func Initialize() {
	var err error
	redisConnectionPool, err = pool.New("tcp", helper.CONFIG.RedisAddress,
		helper.CONFIG.RedisConnectionNumber)
	if err != nil {
		panic("Failed to connect to Redis server: " + err.Error())
	}
}

func Close() {
	redisConnectionPool.Empty()
}

func GetClient() (*redis.Client, error) {
	return redisConnectionPool.Get()
}

func PutClient(c *redis.Client) {
	redisConnectionPool.Put(c)
}

func Remove(table RedisDatabase, key string) (err error) {
	c, err := GetClient()
	if err != nil {
		return err
	}
	defer PutClient(c)

	// Use table.String() + key as Redis key
	return c.Cmd("del", table.String()+key).Err
}

func Set(table RedisDatabase, key string, value interface{}) (err error) {
	c, err := GetClient()
	if err != nil {
		return err
	}
	defer PutClient(c)

	encodedValue, err := json.Marshal(value)
	if err != nil {
		return err
	}
	// Use table.String() + key as Redis key
	return c.Cmd("set", table.String()+key, string(encodedValue)).Err
}

func Get(table RedisDatabase, key string,
	unmarshal func([]byte) (interface{}, error)) (value interface{}, err error) {

	c, err := GetClient()
	if err != nil {
		return
	}
	defer PutClient(c)

	// Use table.String() + key as Redis key
	encodedValue, err := c.Cmd("get", table.String()+key).Bytes()
	if err != nil {
		return
	}
	return unmarshal(encodedValue)
}

// Get file bytes
// `start` and `end` are inclusive
// FIXME: this API causes an extra memory copy, need to patch radix to fix it
func GetBytes(key string, start int64, end int64) ([]byte, error) {
	c, err := GetClient()
	if err != nil {
		return nil, err
	}
	defer PutClient(c)

	// Note Redis returns "" for nonexist key for GETRANGE
	return c.Cmd("getrange", FileTable.String()+key, start, end).Bytes()
}

// Set file bytes
func SetBytes(key string, value []byte) (err error) {
	c, err := GetClient()
	if err != nil {
		return err
	}
	defer PutClient(c)

	// Use table.String() + key as Redis key
	return c.Cmd("set", FileTable.String()+key, value).Err
}

// Publish the invalid message to other YIG instances through Redis
func Invalid(table RedisDatabase, key string) (err error) {
	c, err := GetClient()
	if err != nil {
		return err
	}
	defer PutClient(c)

	return c.Cmd("publish", table.InvalidQueue(), key).Err
}
