package redis

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/go-redis/redis_rate/v8"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/bsm/redislock"
	"github.com/cep21/circuit"
	"github.com/go-redis/redis/v7"
	"github.com/journeymidnight/yig/circuitbreak"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/minio/highwayhash"
)

type Redis interface {
	Close()
	Set(table RedisDatabase, key string, value interface{}) error
	Get(table RedisDatabase, key string,
		unmarshal func([]byte) (interface{}, error)) (interface{}, error)
	Remove(table RedisDatabase, key string) error

	// Get Usages
	// `start` and `end` are inclusive
	GetUsage(key string) (string, error)

	// Set file bytes
	SetBytes(key string, value []byte) (err error)

	// Get file bytes
	// `start` and `end` are inclusive
	// FIXME: this API causes an extra memory copy, need to patch radix to fix it
	GetBytes(key string, start int64, end int64) ([]byte, error)

	// Publish the invalid message to other YIG instances through Redis
	Invalid(table RedisDatabase, key string) (err error)

	Check()
}

var RedisConn Redis
var RedisClient redislock.RedisClient
var QosLimiter *redis_rate.Limiter
var Locker *redislock.Client

const (
	InvalidQueueName = "InvalidQueue"
	keyvalue         = "000102030405060708090A0B0C0D0E0FF0E0D0C0B0A090807060504030201000" // This is the key for hash sum !
)

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

func GenMutexKey(object *types.Object) string {
	return object.BucketName + ":" + object.ObjectId + ":" + object.VersionId
}

func Initialize() {
	switch helper.CONFIG.RedisStore {
	case "single":
		helper.Logger.Info("Redis Mode Single, ADDR is:", helper.CONFIG.RedisAddress)
		r := InitializeSingle()
		RedisConn = r.(Redis)
	case "cluster":
		helper.Logger.Info("Redis Mode Cluster, ADDRs is:", helper.CONFIG.RedisGroup)
		r := InitializeCluster()
		RedisConn = r.(Redis)
	}
	Locker = redislock.New(RedisClient)
}

type SingleRedis struct {
	client  *redis.Client
	circuit *circuit.Circuit
}

var (
	client *redis.Client
	cb     *circuit.Circuit
)

func InitializeSingle() interface{} {
	options := &redis.Options{
		Addr:         helper.CONFIG.RedisAddress,
		MaxRetries:   helper.CONFIG.RedisMaxRetries,
		DialTimeout:  time.Duration(helper.CONFIG.RedisConnectTimeout) * time.Second,
		ReadTimeout:  time.Duration(helper.CONFIG.RedisReadTimeout) * time.Second,
		WriteTimeout: time.Duration(helper.CONFIG.RedisWriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(helper.CONFIG.RedisPoolIdleTimeout) * time.Second,
	}
	if helper.CONFIG.RedisPassword != "" {
		options.Password = helper.CONFIG.RedisPassword
	}
	cb = circuitbreak.NewCacheCircuit()
	client = redis.NewClient(options)
	RedisClient = client
	QosLimiter = redis_rate.NewLimiter(client)
	r := &SingleRedis{
		client:  client,
		circuit: cb,
	}
	return interface{}(r)
}

func (s *SingleRedis) Close() {
	if err := s.client.Close(); err != nil {
		helper.Logger.Error("Cannot close redis client:", err)
	}
}

func (s *SingleRedis) Set(table RedisDatabase, key string, value interface{}) error {
	return s.circuit.Execute(
		context.Background(),
		func(ctx context.Context) error {
			c := s.client.WithContext(ctx)
			conn := c.Conn()
			defer conn.Close()
			encodedValue, err := helper.MsgPackMarshal(value)
			if err != nil {
				return err
			}
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			r, err := conn.Set(table.String()+hashkey, string(encodedValue), 30*time.Second).Result()
			if err == redis.Nil {
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

func (s *SingleRedis) Get(table RedisDatabase, key string,
	unmarshal func([]byte) (interface{}, error)) (value interface{}, err error) {
	var encodedValue []byte
	err = s.circuit.Execute(
		context.Background(),
		func(ctx context.Context) error {
			c := s.client.WithContext(ctx)
			conn := c.Conn()
			defer conn.Close()
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			encodedValue, err = conn.Get(table.String() + hashkey).Bytes()
			if err != nil {
				if err == redis.Nil {
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

func (s *SingleRedis) Remove(table RedisDatabase, key string) error {
	return s.circuit.Execute(
		context.Background(),
		func(ctx context.Context) error {
			c := s.client.WithContext(ctx)
			conn := c.Conn()
			defer conn.Close()
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			_, err = conn.Del(table.String() + hashkey).Result()
			if err == redis.Nil {
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

func (s *SingleRedis) GetUsage(key string) (value string, err error) {
	var encodedValue string
	err = s.circuit.Execute(
		context.Background(),
		func(ctx context.Context) error {
			c := s.client.WithContext(ctx)
			conn := c.Conn()
			defer conn.Close()
			encodedValue, err = conn.Get(key).Result()
			if err != nil {
				if err == redis.Nil {
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
	return encodedValue, nil
}

func (s *SingleRedis) SetBytes(key string, value []byte) (err error) {
	return s.circuit.Execute(
		context.Background(),
		func(ctx context.Context) error {
			c := s.client.WithContext(ctx)
			conn := c.Conn()
			defer conn.Close()
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			r, err := conn.Set(FileTable.String()+hashkey, value, 0).Result()
			if err == redis.Nil {
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

func (s *SingleRedis) GetBytes(key string, start int64, end int64) ([]byte, error) {
	var value []byte
	err := s.circuit.Execute(
		context.Background(),
		func(ctx context.Context) error {
			c := s.client.WithContext(ctx)
			conn := c.Conn()
			defer conn.Close()
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			value, err = conn.GetRange(FileTable.String()+hashkey, start, end).Bytes()
			if err != nil {
				if err == redis.Nil {
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

func (s *SingleRedis) Invalid(table RedisDatabase, key string) (err error) {
	return s.circuit.Execute(
		context.Background(),
		func(ctx context.Context) error {
			c := s.client.WithContext(ctx)
			conn := c.Conn()
			defer conn.Close()
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			r, err := conn.Publish(table.InvalidQueue(), hashkey).Result()
			if err == redis.Nil {
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

func (s *SingleRedis) Check() {
	s.circuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			_, err = s.client.Ping().Result()
			if err != nil {
				helper.Logger.Error("Ping redis error:", err)
			}
			return err
		},
		nil,
	)
	if s.circuit.IsOpen() {
		helper.Logger.Warn(circuitbreak.CacheCircuitIsOpenErr)
	}
}

type ClusterRedis struct {
	cluster *redis.ClusterClient
	circuit *circuit.Circuit
}

var cluster *redis.ClusterClient

func InitializeCluster() interface{} {
	clusterRedis := &redis.ClusterOptions{
		Addrs:        helper.CONFIG.RedisGroup,
		DialTimeout:  time.Duration(helper.CONFIG.RedisConnectTimeout) * time.Second,
		ReadTimeout:  time.Duration(helper.CONFIG.RedisReadTimeout) * time.Second,
		WriteTimeout: time.Duration(helper.CONFIG.RedisWriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(helper.CONFIG.RedisPoolIdleTimeout) * time.Second,
		MinIdleConns: helper.CONFIG.RedisMinIdleConns,
	}

	if helper.CONFIG.RedisPassword != "" {
		clusterRedis.Password = helper.CONFIG.RedisPassword
	}

	cb = circuitbreak.NewCacheCircuit()
	cluster = redis.NewClusterClient(clusterRedis)
	RedisClient = cluster
	QosLimiter = redis_rate.NewLimiter(cluster)
	r := &ClusterRedis{
		cluster: cluster,
		circuit: cb,
	}
	return interface{}(r)
}

func (c *ClusterRedis) Close() {
	if err := c.cluster.Close(); err != nil {
		helper.Logger.Error("Cannot close redis cluster client:", err)
	}
}

func (c *ClusterRedis) Set(table RedisDatabase, key string, value interface{}) error {
	return c.circuit.Execute(
		context.Background(),
		func(ctx context.Context) error {
			conn := c.cluster.WithContext(ctx)
			encodedValue, err := helper.MsgPackMarshal(value)
			if err != nil {
				return err
			}
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			r, err := conn.Set(table.String()+hashkey, string(encodedValue), 30*time.Second).Result()
			if err == redis.Nil {
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

func (c *ClusterRedis) Get(table RedisDatabase, key string,
	unmarshal func([]byte) (interface{}, error)) (value interface{}, err error) {
	var encodedValue []byte
	err = c.circuit.Execute(
		context.Background(),
		func(ctx context.Context) error {
			conn := c.cluster.WithContext(ctx)
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			encodedValue, err = conn.Get(table.String() + hashkey).Bytes()
			if err != nil {
				if err == redis.Nil {
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

func (c *ClusterRedis) Remove(table RedisDatabase, key string) error {
	return c.circuit.Execute(
		context.Background(),
		func(ctx context.Context) error {
			conn := c.cluster.WithContext(ctx)
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			_, err = conn.Del(table.String() + hashkey).Result()
			if err == redis.Nil {
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

func (c *ClusterRedis) GetUsage(key string) (value string, err error) {
	var encodedValue string
	err = c.circuit.Execute(
		context.Background(),
		func(ctx context.Context) error {
			conn := c.cluster.WithContext(ctx)
			encodedValue, err = conn.Get(key).Result()
			if err != nil {
				if err == redis.Nil {
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
	return encodedValue, nil
}

func (c *ClusterRedis) SetBytes(key string, value []byte) (err error) {
	return c.circuit.Execute(
		context.Background(),
		func(ctx context.Context) error {
			conn := c.cluster.WithContext(ctx)
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			r, err := conn.Set(FileTable.String()+hashkey, value, 0).Result()
			if err == redis.Nil {
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

func (c *ClusterRedis) GetBytes(key string, start int64, end int64) ([]byte, error) {
	var value []byte
	err := c.circuit.Execute(
		context.Background(),
		func(ctx context.Context) error {
			conn := c.cluster.WithContext(ctx)
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			value, err = conn.GetRange(FileTable.String()+hashkey, start, end).Bytes()
			if err != nil {
				if err == redis.Nil {
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

func (c *ClusterRedis) Invalid(table RedisDatabase, key string) (err error) {
	return c.circuit.Execute(
		context.Background(),
		func(ctx context.Context) error {
			conn := c.cluster.WithContext(ctx)
			hashkey, err := HashSum(key)
			if err != nil {
				return err
			}
			// Use table.String() + hashkey as Redis key
			r, err := conn.Publish(table.InvalidQueue(), hashkey).Result()
			if err == redis.Nil {
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

func (c *ClusterRedis) Check() {
	c.circuit.Execute(
		context.Background(),
		func(ctx context.Context) (err error) {
			_, err = c.cluster.Ping().Result()
			if err != nil {
				helper.Logger.Error("Ping redis error:", err)
			}
			return err
		},
		nil,
	)
	if c.circuit.IsOpen() {
		helper.Logger.Warn(circuitbreak.CacheCircuitIsOpenErr)
	}
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
