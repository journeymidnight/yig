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
			Addrs:        helper.CONFIG.RedisNodes,
			ReadTimeout:  time.Duration(helper.CONFIG.RedisReadTimeout) * time.Second,
			DialTimeout:  time.Duration(helper.CONFIG.RedisConnectTimeout) * time.Second,
			WriteTimeout: time.Duration(helper.CONFIG.RedisWriteTimeout) * time.Second,
			IdleTimeout:  time.Duration(helper.CONFIG.RedisKeepAlive) * time.Second,
		}
		if helper.CONFIG.RedisConnectionNumber > 0 {
			options.PoolSize = helper.CONFIG.RedisConnectionNumber
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
		if helper.CONFIG.RedisConnectionNumber > 0 {
			options.PoolSize = helper.CONFIG.RedisConnectionNumber
		}
		if helper.CONFIG.RedisPassword != "" {
			options.Password = helper.CONFIG.RedisPassword
		}
		cli.redisClient = redis.NewFailoverClient(options)
		cli.clientType = REDIS_SENTINEL_CLIENT
	default:
		options := &redis.Options{
			Addr:         helper.CONFIG.RedisAddress,
			ReadTimeout:  time.Duration(helper.CONFIG.RedisReadTimeout) * time.Second,
			DialTimeout:  time.Duration(helper.CONFIG.RedisConnectTimeout) * time.Second,
			WriteTimeout: time.Duration(helper.CONFIG.RedisWriteTimeout) * time.Second,
			IdleTimeout:  time.Duration(helper.CONFIG.RedisKeepAlive) * time.Second,
		}

		if helper.CONFIG.RedisConnectionNumber > 0 {
			options.PoolSize = helper.CONFIG.RedisConnectionNumber
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

func (cli *RedisCli) Keys(pattern string) ([]string, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.Keys(pattern).Result()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.Keys(pattern).Result()
	default:
		return nil, errors.New(ERR_NOT_INIT_MSG)
	}
}

func (cli *RedisCli) MGet(keys []string) ([]interface{}, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.MGet(keys...).Result()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.MGet(keys...).Result()
	default:
		return nil, errors.New(ERR_NOT_INIT_MSG)
	}
}

func (cli *RedisCli) MSet(pairs map[interface{}]interface{}) (string, error) {
	var pairList []interface{}

	for k, v := range pairs {
		pairList = append(pairList, k, v)
	}

	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.MSet(pairList...).Result()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.MSet(pairList...).Result()
	default:
		return "", errors.New(ERR_NOT_INIT_MSG)
	}
}

func (cli *RedisCli) IncrBy(key string, value int64) (int64, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.IncrBy(key, value).Result()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.IncrBy(key, value).Result()
	default:
		return 0, errors.New(ERR_NOT_INIT_MSG)
	}
}

func (cli *RedisCli) Expire(key string, expire int64) (bool, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.Expire(key, time.Duration(expire)*time.Millisecond).Result()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.Expire(key, time.Duration(expire)*time.Millisecond).Result()
	default:
		return false, errors.New(ERR_NOT_INIT_MSG)
	}
}

/***************** below are hashes commands *************************/

func (cli *RedisCli) HSet(key, field string, value interface{}) (bool, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.HSet(key, field, value).Result()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.HSet(key, field, value).Result()
	default:
		return false, errors.New(ERR_NOT_INIT_MSG)
	}
}

func (cli *RedisCli) HGet(key, field string) (string, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.HGet(key, field).Result()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.HGet(key, field).Result()
	default:
		return "", errors.New(ERR_NOT_INIT_MSG)
	}
}

func (cli *RedisCli) HGetInt64(key, field string) (int64, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.HGet(key, field).Int64()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.HGet(key, field).Int64()
	default:
		return 0, errors.New(ERR_NOT_INIT_MSG)
	}
}

func (cli *RedisCli) HGetAll(key string) (map[string]string, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.HGetAll(key).Result()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.HGetAll(key).Result()
	default:
		return nil, errors.New(ERR_NOT_INIT_MSG)
	}
}

func (cli *RedisCli) HIncrBy(key, field string, incr int64) (int64, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.HIncrBy(key, field, incr).Result()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.HIncrBy(key, field, incr).Result()
	default:
		return 0, errors.New(ERR_NOT_INIT_MSG)
	}
}

func (cli *RedisCli) HMSet(key string, fields map[string]interface{}) (string, error) {
	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		return cli.redisClient.HMSet(key, fields).Result()
	case REDIS_CLUSTER_CLIENT:
		return cli.redisClusterClient.HMSet(key, fields).Result()
	default:
		return "", errors.New(ERR_NOT_INIT_MSG)
	}
}

func (cli *RedisCli) HMGet(key string, fields []string) (map[string]interface{}, error) {
	results := make(map[string]interface{})
	var values []interface{}
	var err error

	switch cli.clientType {
	case REDIS_NORMAL_CLIENT, REDIS_SENTINEL_CLIENT:
		values, err = cli.redisClient.HMGet(key, fields...).Result()
	case REDIS_CLUSTER_CLIENT:
		values, err = cli.redisClusterClient.HMGet(key, fields...).Result()
	default:
		return nil, errors.New(ERR_NOT_INIT_MSG)
	}
	if err != nil {
		helper.Logger.Println(2, "failed to HMGet for key ", key, " with err: ", err)
		return nil, err
	}

	if len(fields) != len(values) {
		helper.Logger.Println(2, "panic HMGet, input fields number: ", len(fields), " got values number: ",
			len(values))
		return nil, errors.New("HMGet fields number is not equal to values number.")
	}

	for i, key := range fields {
		results[key] = values[i]
	}
	return results, nil
}
