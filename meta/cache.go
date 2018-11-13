package meta

import (
	"container/list"
	"sync"
	"time"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/redis"
	"github.com/mediocregopher/radix.v2/pubsub"
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

// metadata is organized in 3 layers: YIG instance memory, Redis, HBase
// `MetaCache` forces "Cache-Aside Pattern", see https://msdn.microsoft.com/library/dn589799.aspx
type enabledMetaCache struct {
	lock       *sync.Mutex // protects both `lruList` and `cache`
	MaxEntries int
	lruList    *list.List
	Hit        int64
	Miss       int64
	// maps table -> key -> value
	cache                       map[redis.RedisDatabase]map[string]*list.Element
	failedCacheInvalidOperation chan entry
}

type disabledMetaCache struct{}

type entry struct {
	table redis.RedisDatabase
	key   string
	value interface{}
}

func newMetaCache(myType CacheType) (m MetaCache) {

	helper.Logger.Printf(10, "Setting Up Metadata Cache: %s\n", cacheNames[int(myType)])

	if myType == EnableCache {
		m := &enabledMetaCache{
			lock:       new(sync.Mutex),
			MaxEntries: helper.CONFIG.InMemoryCacheMaxEntryCount,
			lruList:    list.New(),
			cache:      make(map[redis.RedisDatabase]map[string]*list.Element),
			Hit:        0,
			Miss:       0,
			failedCacheInvalidOperation: make(chan entry, helper.CONFIG.RedisConnectionNumber),
		}
		for _, table := range redis.MetadataTables {
			m.cache[table] = make(map[string]*list.Element)
		}
		go invalidLocalCache(m)
		go invalidRedisCache(m)
		return m
	} else if myType == SimpleCache {
		m := new(enabledSimpleMetaCache)
		m.Hit = 0
		m.Miss = 0
		return m
	}
	return &disabledMetaCache{}
}

// subscribe to Redis channels and handle cache invalid info
func invalidLocalCache(m *enabledMetaCache) {
	c, err := redis.GetClient()
	if err != nil {
		helper.Logger.Panicln(0, "Connot get Redis client: "+err.Error())
	}

	subClient := pubsub.NewSubClient(c)
	subClient.PSubscribe(redis.InvalidQueueName + "*")
	for {
		response := subClient.Receive() // should block
		if response.Err != nil {
			if !response.Timeout() {
				helper.Logger.Println(5, "Error receiving from redis channel:",
					response.Err)
			}
			continue
		}

		table, err := redis.TableFromChannelName(response.Channel)
		if err != nil {
			helper.Logger.Println(5, "Bad redis channel name: ", response.Channel)
			continue
		}
		m.remove(table, response.Message)
	}
}

// redo failed invalid operation in enabledMetaCache.failedCacheInvalidOperation channel
func invalidRedisCache(m *enabledMetaCache) {
	for {
		failedEntry := <-m.failedCacheInvalidOperation
		err := redis.Remove(failedEntry.table, failedEntry.key)
		if err != nil {
			m.failedCacheInvalidOperation <- failedEntry
			time.Sleep(1 * time.Second)
			continue
		}
		err = redis.Invalid(failedEntry.table, failedEntry.key)
		if err != nil {
			m.failedCacheInvalidOperation <- failedEntry
			time.Sleep(1 * time.Second)
		}
	}
}

func (m *enabledMetaCache) invalidRedisCache(table redis.RedisDatabase, key string) {
	err := redis.Invalid(table, key)
	if err != nil {
		m.failedCacheInvalidOperation <- entry{
			table: table,
			key:   key,
		}
	}
}

func (m *enabledMetaCache) set(table redis.RedisDatabase, key string, value interface{}) {
	m.lock.Lock()
	if element, ok := m.cache[table][key]; ok {
		m.lruList.MoveToFront(element)
		element.Value.(*entry).value = value
		m.lock.Unlock()
		return
	}
	element := m.lruList.PushFront(&entry{table, key, value})
	m.cache[table][key] = element
	m.lock.Unlock()

	if m.lruList.Len() > m.MaxEntries {
		m.removeOldest()
	}
}

// Forces "cache-aside" pattern, calls `onCacheMiss` when key is missed from
// both memory and Redis, use `unmarshal` get expected type from Redis
func (m *enabledMetaCache) Get(table redis.RedisDatabase, key string,
	onCacheMiss func() (interface{}, error),
	unmarshaller func([]byte) (interface{}, error), willNeed bool) (value interface{}, err error) {

	helper.Logger.Println(10, "enabledMetaCache Get()", table, key)

	m.lock.Lock()
	if element, hit := m.cache[table][key]; hit {
		m.lruList.MoveToFront(element)
		defer m.lock.Unlock()
		m.Hit = m.Hit + 1

		return element.Value.(*entry).value, nil
	}
	m.lock.Unlock()

	value, err = redis.Get(table, key, unmarshaller)
	if err == nil && value != nil {
		if willNeed == true {
			m.set(table, key, value)
		}
		m.Hit = m.Hit + 1
		return value, nil
	}

	//if redis doesn't have the entry
	if onCacheMiss != nil {
		value, err = onCacheMiss()
		if err != nil {
			return
		}

		if willNeed == true {
			err = redis.Set(table, key, value)
			if err != nil {
				// invalid the entry asynchronously
				m.failedCacheInvalidOperation <- entry{
					table: table,
					key:   key,
				}
			}
			m.invalidRedisCache(table, key)
			m.set(table, key, value)
		}

		m.Miss = m.Miss + 1
		return value, nil
	}
	return nil, nil
}

func (m *disabledMetaCache) Get(table redis.RedisDatabase, key string,
	onCacheMiss func() (interface{}, error),
	unmarshaller func([]byte) (interface{}, error), willNeed bool) (value interface{}, err error) {

	return onCacheMiss()
}

func (m *enabledMetaCache) remove(table redis.RedisDatabase, key string) {
	helper.Logger.Println(10, "enabledMetaCache Remove()", table, key)

	m.lock.Lock()
	element, hit := m.cache[table][key]
	if hit {
		m.lruList.Remove(element)
		delete(m.cache[table], key)
	}
	m.lock.Unlock()
}

func (m *enabledMetaCache) Remove(table redis.RedisDatabase, key string) {
	err := redis.Remove(table, key)

	if err != nil {
		// invalid the entry asynchronously
		m.failedCacheInvalidOperation <- entry{
			table: table,
			key:   key,
		}
	}
	m.invalidRedisCache(table, key)
	// this would cause YIG instance handling the API request to call `remove` twice
	// (another time is when getting cache invalid message from Redis), but let it be
	m.remove(table, key)
}

func (m *disabledMetaCache) Remove(table redis.RedisDatabase, key string) {
	return
}

func (m *enabledMetaCache) removeOldest() {
	m.lock.Lock()
	element := m.lruList.Back()
	if element != nil {
		toInvalid := element.Value.(*entry)
		m.lruList.Remove(element)
		delete(m.cache[toInvalid.table], toInvalid.key)
	}
	m.lock.Unlock()

	// Do not invalid Redis cache because data there is still _valid_
}

func (m *enabledMetaCache) GetCacheHitRatio() float64 {
	return float64(m.Hit) / float64(m.Hit+m.Miss)
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

	helper.Logger.Println(10, "enabledMetaCache Get()", table, key)

	value, err = redis.Get(table, key, unmarshaller)
	if err == nil && value != nil {
		m.Hit = m.Hit + 1
		return value, nil
	}

	//if redis doesn't have the entry
	if onCacheMiss != nil {
		value, err = onCacheMiss()
		if err != nil {
			return
		}

		if willNeed == true {
			err = redis.Set(table, key, value)
			if err != nil {
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
