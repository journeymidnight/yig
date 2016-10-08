package redis

import (
	"git.letv.cn/yig/yig/helper"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
)

type RedisDatabase int

const (
	UserTable RedisDatabase = iota
	BucketTable
	ObjectTable
	FileTable
	FilePartTable
)

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
