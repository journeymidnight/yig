package cache

import (
	"github.com/journeymidnight/yig/iam/common"
	"sync"
	"time"
)

const (
	CACHE_EXPIRE_TIME = 600 * time.Second
	CACHE_CHECK_TIME  = 60 * time.Second
)

type cacheEntry struct {
	createTime time.Time
	credential common.Credential
}

// maps access key to Credential object
type cache struct {
	cache map[string]cacheEntry
	lock  *sync.RWMutex
}

var IamCache *cache

func cacheInvalidator() {
	if IamCache == nil {
		panic("IAM cache not initialized yet")
	}
	for {
		keysToExpire := make([]string, 0)
		now := time.Now()
		IamCache.lock.Lock()
		for k, entry := range IamCache.cache {
			if entry.createTime.Add(CACHE_EXPIRE_TIME).Before(now) {
				keysToExpire = append(keysToExpire, k)
			}
		}
		for _, key := range keysToExpire {
			delete(IamCache.cache, key)
		}
		IamCache.lock.Unlock()
		time.Sleep(CACHE_CHECK_TIME)
	}
}

func InitializeIamCache() {
	if IamCache != nil {
		return
	}
	IamCache = &cache{
		cache: make(map[string]cacheEntry),
		lock:  new(sync.RWMutex),
	}
	go cacheInvalidator()
}

func (c *cache) Get(key string) (credential common.Credential, hit bool) {
	c.lock.RLock()
	entry, hit := c.cache[key]
	c.lock.RUnlock()
	if hit {
		credential = entry.credential
	}
	return credential, hit
}

func (c *cache) Set(key string, credential common.Credential) {
	entry := cacheEntry{
		createTime: time.Now(),
		credential: credential,
	}
	c.lock.Lock()
	c.cache[key] = entry
	c.lock.Unlock()
}
