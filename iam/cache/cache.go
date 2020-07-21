package cache

import (
	"sync"
	"time"

	"github.com/journeymidnight/yig/iam/common"
)

const (
	CACHE_EXPIRE_TIME = 600 * time.Second
	CACHE_CHECK_TIME  = 60 * time.Second
)

type CacheEntry struct {
	CreateTime time.Time
	Credential common.Credential
}

var IamCache sync.Map // accessKey -> cacheEntry

func cacheInvalidator() {

	for {
		var keysToExpire []string
		now := time.Now()
		IamCache.Range(func(key, value interface{}) bool {
			k := key.(string)
			entry := value.(CacheEntry)
			if entry.CreateTime.Add(CACHE_EXPIRE_TIME).Before(now) {
				keysToExpire = append(keysToExpire, k)
			}
			return true
		})

		for _, key := range keysToExpire {
			IamCache.Delete(key)
		}
		time.Sleep(CACHE_CHECK_TIME)
	}
}

func InitializeIamCache() {
	go cacheInvalidator()
}
