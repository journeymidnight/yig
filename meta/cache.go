package meta

import (
	"database/sql"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/redis"
)

type CacheType int

const (
	NoCache CacheType = iota
	EnableCache
	SimpleCache
)

var cacheNames = [...]string{"NOCACHE", "EnableCache", "SimpleCache"}

type MetaCache interface {
	Get(table redis.RedisDatabase, key string,
		onCacheMiss func() (interface{}, error),
		unmarshaller func([]byte) (interface{}, error), willNeed bool) (value interface{}, err error)
	Remove(table redis.RedisDatabase, key string)
	GetCacheHitRatio() float64
}

type disabledMetaCache struct{}

type entry struct {
	table redis.RedisDatabase
	key   string
	value interface{}
}

func newMetaCache(myType CacheType) (m MetaCache) {

	helper.Logger.Info("Setting Up Metadata Cache:", cacheNames[int(myType)])
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
	unmarshaller func([]byte) (interface{}, error), willNeed bool) (value interface{}, err error) {

	return onCacheMiss()
}

func (m *disabledMetaCache) Remove(table redis.RedisDatabase, key string) {
	return
}

func (m *disabledMetaCache) GetCacheHitRatio() float64 {
	return -1
}

type enabledSimpleMetaCache struct {
	Hit  int64
	Miss int64
}

func (m *enabledSimpleMetaCache) Get(table redis.RedisDatabase, key string,
	onCacheMiss func() (interface{}, error),
	unmarshaller func([]byte) (interface{}, error), willNeed bool) (value interface{}, err error) {

	helper.Logger.Info("enabledSimpleMetaCache.Get table:", table, "key:", key)
	value, err = redis.Get(table, key, unmarshaller)
	if err != nil {
		helper.Logger.Info("enabledSimpleMetaCache.Get err:", err,
			"table:", table, "key:", key)
	}
	if err == nil && value != nil {
		m.Hit = m.Hit + 1
		return value, nil
	}

	// if redis doesn't have the entry
	if onCacheMiss != nil {
		value, err = onCacheMiss()
		if err != nil {
			if err != sql.ErrNoRows {
				helper.Logger.Warn("exec onCacheMiss() err:", err)
			}
			return
		}

		if willNeed == true {
			err = redis.Set(table, key, value)
			if err != nil {
				helper.Logger.Warn("redis is down!")
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
