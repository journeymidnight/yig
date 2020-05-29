package redis

import (
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v7"
	"github.com/journeymidnight/yig-restore/helper"
	"github.com/journeymidnight/yig-restore/meta/types"
)

type Redis interface {
	Remove(key string)
	Close()
}

func Remove(key string) {
	RedisConn.Remove(key)
}

func Close() {
	RedisConn.Close()
}

var (
	client      *redis.Client
	cluster     *redis.ClusterClient
	RedisConn   Redis
	RedisClient redislock.RedisClient
	Locker      *redislock.Client
)

const RESTOREINFO = "Restore:"

func Initialize() {
	switch helper.Conf.RedisStore {
	case "single":
		helper.Logger.Info("Redis Mode Single, ADDR is:", helper.Conf.RedisAddress)
		r := InitializeSingle()
		RedisConn = r.(Redis)
	case "cluster":
		helper.Logger.Info("Redis Mode Cluster, ADDRs is:", helper.Conf.RedisGroup)
		r := InitializeCluster()
		RedisConn = r.(Redis)
	default:
		helper.Logger.Info("Redis Mode Single, ADDR is:", helper.Conf.RedisAddress)
		r := InitializeSingle()
		RedisConn = r.(Redis)
	}
	Locker = redislock.New(RedisClient)
}

func InitializeSingle() interface{} {
	options := &redis.Options{
		Addr:         helper.Conf.RedisAddress,
		MaxRetries:   helper.Conf.RedisMaxRetries,
		DialTimeout:  time.Duration(helper.Conf.RedisConnectTimeout) * time.Second,
		ReadTimeout:  time.Duration(helper.Conf.RedisReadTimeout) * time.Second,
		WriteTimeout: time.Duration(helper.Conf.RedisWriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(helper.Conf.RedisPoolIdleTimeout) * time.Second,
	}
	if helper.Conf.RedisPassword != "" {
		options.Password = helper.Conf.RedisPassword
	}
	client = redis.NewClient(options)
	_, err := client.Ping().Result()
	if err != nil {
		helper.Logger.Error("Failed to connect to redis, err is :", err)
		panic(err)
	}
	RedisClient = client
	r := &SingleRedis{client: client}
	return interface{}(r)
}

type SingleRedis struct {
	client *redis.Client
}

func (s *SingleRedis) Remove(key string) {
	_, err := s.client.Del(key).Result()
	if err != nil {
		helper.Logger.Error("Failed to delete redis object", err)
	}
}

func (s *SingleRedis) Close() {
	err := s.client.Close()
	if err != nil {
		helper.Logger.Error("Failed to close redis connection", err)
	}
}

func InitializeCluster() interface{} {
	clusterRedis := &redis.ClusterOptions{
		Addrs:        helper.Conf.RedisGroup,
		DialTimeout:  time.Duration(helper.Conf.RedisConnectTimeout) * time.Second,
		ReadTimeout:  time.Duration(helper.Conf.RedisReadTimeout) * time.Second,
		WriteTimeout: time.Duration(helper.Conf.RedisWriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(helper.Conf.RedisPoolIdleTimeout) * time.Second,
	}

	if helper.Conf.RedisPassword != "" {
		clusterRedis.Password = helper.Conf.RedisPassword
	}

	cluster = redis.NewClusterClient(clusterRedis)
	_, err := cluster.Ping().Result()
	if err != nil {
		helper.Logger.Error("Failed to connect to redis, err is :", err)
		panic(err)
	}
	RedisClient = cluster
	r := &ClusterRedis{
		cluster: cluster,
	}
	return interface{}(r)
}

type ClusterRedis struct {
	cluster *redis.ClusterClient
}

func (c *ClusterRedis) Remove(key string) {
	_, err := c.cluster.Del(key).Result()
	if err != nil {
		helper.Logger.Error("Failed to delete redis object", err)
	}
}

func (c *ClusterRedis) Close() {
	err := c.cluster.Close()
	if err != nil {
		helper.Logger.Error("Failed to close redis connection", err)
	}
}

func GenMutexKey(freezer *types.Freezer) string {
	return RESTOREINFO + freezer.BucketName + ":" + freezer.Name + ":" + freezer.VersionId
}
