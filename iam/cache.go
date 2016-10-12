package iam

import (
	"time"
	"sync"
)

const (
	CACHE_EXPIRE_TIME = 60 * time.Second
)

type cacheEntry struct {
	createTime time.Time
	credential Credential
}

// maps access key to Credential object
type cache struct{
	cache map[string]cacheEntry
	lock *sync.Mutex
}

var iamCache *cache

func cacheInvalidator() {
	if iamCache == nil {
		panic("IAM cache not initialized yet")
	}
	for {
		keysToExpire := make([]string, 0)
		now := time.Now()
		iamCache.lock.Lock()
		for k, entry := range iamCache.cache {
			if entry.createTime.Add(CACHE_EXPIRE_TIME).After(now) {
				keysToExpire = append(keysToExpire, k)
			}
		}
		for _, key := range keysToExpire {
			delete(iamCache.cache, key)
		}
		iamCache.lock.Unlock()
		time.Sleep(CACHE_EXPIRE_TIME)
	}
}

func initializeIamCache() {
	if iamCache != nil {
		return
	}
	iamCache = &cache{
		cache: make(map[string]cacheEntry),
		lock: new(sync.Mutex),
	}
	go cacheInvalidator()
}

func (c *cache) get(key string) (credential Credential, hit bool) {
	entry, hit := c.cache[key]
	if hit {
		credential = entry.credential
	}
	return credential, hit
}

func (c *cache) set(key string, credential Credential) {
	entry := cacheEntry{
		createTime: time.Now(),
		credential: credential,
	}
	c.cache[key] = entry
}

