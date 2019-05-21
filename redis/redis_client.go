package redis

import (
	"errors"
	"time"

	"github.com/go-redis/redis"
	"github.com/journeymidnight/yig/helper"
)

const (
	REDIS_UNKNOWN_CLIENT = iota
	REDIS_NORMAL_CLIENT
	REDIS_CLUSTER_CLIENT
	REDIS_SENTINEL_CLIENT
)

const ERR_NOT_INIT_MSG = "redis client is not initialized yet."

type RedisCli struct {
	clientType         int
	redisClient        *redis.Client
	redisClusterClient *redis.ClusterClient
}

func NewRedisCli() *RedisCli {
	return &RedisCli{
		clientType: REDIS_UNKNOWN_CLIENT,
	}
}

func (cli *RedisCli) Init() {
	switch helper.CONFIG.RedisMode {
	case 1:
		options := &redis.ClusterOptions{
			ReadTimeout:  time.Duration(helper.CONFIG.RedisReadTimeout) * time.Second,
			DialTimeout:  time.Duration(helper.CONFIG.RedisConnectTimeout) * time.Second,
			WriteTimeout: time.Duration(helper.CONFIG.RedisWriteTimeout) * time.Second,
			IdleTimeout:  time.Duration(helper.CONFIG.RedisKeepAlive) * time.Second,
		}
		if helper.CONFIG.RedisPassword != "" {
			options.Password = helper.CONFIG.RedisPassword
		}
		cli.redisClusterClient = redis.NewClusterClient(options)
		cli.clientType = REDIS_CLUSTER_CLIENT
	case 2:
		options := &redis.FailoverOptions{
			MasterName:    helper.CONFIG.RedisSentinelMasterName,
			SentinelAddrs: helper.CONFIG.RedisNodes,
			ReadTimeout:   time.Duration(helper.CONFIG.RedisReadTimeout) * time.Second,
			DialTimeout:   time.Duration(helper.CONFIG.RedisConnectTimeout) * time.Second,
			WriteTimeout:  time.Duration(helper.CONFIG.RedisWriteTimeout) * time.Second,
			IdleTimeout:   time.Duration(helper.CONFIG.RedisKeepAlive) * time.Second,
		}
		if helper.CONFIG.RedisPassword != "" {
			options.Password = helper.CONFIG.RedisPassword
		}
		cli.redisClient = redis.NewFailoverClient(options)
		cli.clientType = REDIS_SENTINEL_CLIENT
	default:
		options := &redis.Options{
			ReadTimeout:  time.Duration(helper.CONFIG.RedisReadTimeout) * time.Second,
			DialTimeout:  time.Duration(helper.CONFIG.RedisConnectTimeout) * time.Second,
			WriteTimeout: time.Duration(helper.CONFIG.RedisWriteTimeout) * time.Second,
			IdleTimeout:  time.Duration(helper.CONFIG.RedisKeepAlive) * time.Second,
		}

		if helper.CONFIG.RedisPassword != "" {
			options.Password = helper.CONFIG.RedisPassword
		}

		cli.redisClient = redis.NewClient(options)
		cli.clientType = REDIS_NORMAL_CLIENT
	}
}

func (cli *RedisCli) IsValid() bool {
	return cli.clientType != REDIS_UNKNOWN_CLIENT
}

func (cli *RedisCli) Close() error {
	switch cli.clientType {
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.Close()
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.Close()
	default:
		return nil
	}
}

func (cli *RedisCli) Del(key string) (int64, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.Del(key).Result()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.Del(key).Result()
	default:
		return 0, errors.New(ERR_NOT_INIT_MSG)
	}
}

/*
* @key: input key
* @value: input value
* @expire: expiration for the key in milliseconds.
 */

func (cli *RedisCli) Set(key string, value interface{}, expire int64) (string, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.Set(key, value, time.Duration(expire)*time.Millisecond).Result()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.Set(key, value, time.Duration(expire)*time.Millisecond).Result()
	default:
		return "", errors.New(ERR_NOT_INIT_MSG)
	}
}

func (cli *RedisCli) Get(key string) ([]byte, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT:
		return cli.redisClient.Get(key).Bytes()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.Get(key).Bytes()
	default:
		return nil, errors.New(ERR_NOT_INIT_MSG)
	}
}

func (cli *RedisCli) GetRange(key string, start, end int64) ([]byte, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.GetRange(key, start, end).Bytes()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.GetRange(key, start, end).Bytes()
	default:
		return nil, errors.New(ERR_NOT_INIT_MSG)
	}
}

func (cli *RedisCli) Publish(channel string, message interface{}) (int64, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.Publish(channel, message).Result()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.Publish(channel, message).Result()
	default:
		return 0, errors.New(ERR_NOT_INIT_MSG)
	}
}

func (cli *RedisCli) Ping() (string, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.Ping().Result()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.Ping().Result()
	default:
		return "", errors.New(ERR_NOT_INIT_MSG)
	}
}
