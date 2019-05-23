package meta

import (
	"database/sql"
	"errors"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/redis"
)

type CacheType int

const (
	NoCache CacheType = iota
	EnableCache
	SimpleCache
)

const (
	MSG_NOT_IMPL = "not implemented."
)

var cacheNames = [...]string{"NOCACHE", "EnableCache", "SimpleCache"}

type MetaCache interface {
	Close()
	Get(table redis.RedisDatabase, key string,
		onCacheMiss func() (interface{}, error),
		willNeed bool) (value interface{}, err error)
	Remove(table redis.RedisDatabase, key string)
	GetCacheHitRatio() float64
	Keys(table redis.RedisDatabase, pattern string) ([]string, error)
	MGet(table redis.RedisDatabase, keys []string) ([]interface{}, error)
	MSet(table redis.RedisDatabase, pairs map[string]interface{}) (string, error)
	IncrBy(table redis.RedisDatabase, key string, value int64) (int64, error)
}

type disabledMetaCache struct{}

type entry struct {
	table redis.RedisDatabase
	key   string
	value interface{}
}

func newMetaCache(myType CacheType) (m MetaCache) {

	helper.Logger.Printf(10, "Setting Up Metadata Cache: %s\n", cacheNames[int(myType)])
	if myType == SimpleCache {
		m := new(enabledSimpleMetaCache)
		m.Hit = 0
		m.Miss = 0
		return m
	}
	return &disabledMetaCache{}
}

func (m *disabledMetaCache) Get(table redis.RedisDatabase, key string,
	onCacheMiss func() (interface{}, error),
	willNeed bool) (value interface{}, err error) {

	return onCacheMiss()
}

func (m *disabledMetaCache) Remove(table redis.RedisDatabase, key string) {
	return
}

func (m *disabledMetaCache) GetCacheHitRatio() float64 {
	return -1
}

func (m *disabledMetaCache) Keys(table redis.RedisDatabase, pattern string) ([]string, error) {
	return nil, errors.New(MSG_NOT_IMPL)
}

func (m *disabledMetaCache) MGet(table redis.RedisDatabase, keys []string) ([]interface{}, error) {
	return nil, errors.New(MSG_NOT_IMPL)
}

func (m *disabledMetaCache) MSet(table redis.RedisDatabase, pairs map[string]interface{}) (string, error) {
	return "", errors.New(MSG_NOT_IMPL)
}

func (m *disabledMetaCache) IncrBy(table redis.RedisDatabase, key string, value int64) (int64, error) {
	return 0, errors.New(MSG_NOT_IMPL)
}

func (m *disabledMetaCache) Close() {
}

type enabledSimpleMetaCache struct {
	Hit  int64
	Miss int64
}

func (m *enabledSimpleMetaCache) Get(table redis.RedisDatabase, key string,
	onCacheMiss func() (interface{}, error), willNeed bool) (value interface{}, err error) {

	helper.Logger.Println(10, "enabledSimpleMetaCache Get. table:", table, "key:", key)

	value, err = redis.Get(table, key)
	if err != nil {
		helper.Logger.Println(5, "enabledSimpleMetaCache Get err:", err, "table:", table, "key:", key)
	}
	if err == nil && value != nil {
		m.Hit = m.Hit + 1
		return value, nil
	}

	//if redis doesn't have the entry
	if onCacheMiss != nil {
		value, err = onCacheMiss()
		if err != nil {
			if err != sql.ErrNoRows {
				helper.ErrorIf(err, "exec onCacheMiss() err.")
			}
			return
		}

		if willNeed == true {
			err = redis.Set(table, key, value)
			if err != nil {
				helper.Logger.Println(5, "WARNING: redis is down!")
				//do nothing, even if redis is down.
			}
		}
		m.Miss = m.Miss + 1
		return value, nil
	}
	return nil, nil
}

func (m *enabledSimpleMetaCache) Remove(table redis.RedisDatabase, key string) {
	redis.Remove(table, key)
}

func (m *enabledSimpleMetaCache) GetCacheHitRatio() float64 {
	return float64(m.Hit) / float64(m.Hit+m.Miss)
}

func (m *enabledSimpleMetaCache) Keys(table redis.RedisDatabase, pattern string) ([]string, error) {
	return redis.Keys(table, pattern)
}

func (m *enabledSimpleMetaCache) MGet(table redis.RedisDatabase, keys []string) ([]interface{}, error) {
	return redis.MGet(table, keys)
}

func (m *enabledSimpleMetaCache) MSet(table redis.RedisDatabase, pairs map[string]interface{}) (string, error) {
	return redis.MSet(table, pairs)
}

func (m *enabledSimpleMetaCache) IncrBy(table redis.RedisDatabase, key string, value int64) (int64, error) {
	return redis.IncrBy(table, key, value)
}

func (m *enabledSimpleMetaCache) Close() {
	redis.Close()
}
