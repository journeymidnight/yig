package redis

import (
	"encoding/json"
	"git.letv.cn/yig/yig/helper"
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
	FilePartTable
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

var MetadataTables = []RedisDatabase{UserTable, BucketTable, ObjectTable}
var DataTables = []RedisDatabase{FileTable, FilePartTable}

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

func Invalid(table RedisDatabase, key string) (err error) {
	c, err := GetClient()
	if err != nil {
		return err
	}
	defer PutClient(c)

	// Use table.String() + key as Redis key
	err = c.Cmd("del", table.String()+key).Err
	if err != nil {
		return err
	}

	err = c.Cmd("publish", table.InvalidQueue(), key).Err
	if err != nil {
		return err
	}
	return nil
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
	err = c.Cmd("set", table.String()+key, string(encodedValue)).Err
	if err != nil {
		return err
	}
	return nil
}

func Get(table RedisDatabase, key string) (value interface{}, err error) {
	c, err := GetClient()
	if err != nil {
		return err
	}
	defer PutClient(c)

	// Use table.String() + key as Redis key
	encodedValue, err := c.Cmd("get", table.String()+key).Bytes()
	if err != nil {
		return err
	}
	err = json.Unmarshal(encodedValue, &value)
	return
}
