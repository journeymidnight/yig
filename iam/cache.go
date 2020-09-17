package iam

import (
	"sync"
	"time"

	error2 "github.com/journeymidnight/yig/error"
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

func reexamineKeys(keys []string) {
	for _, k := range keys {
		credential, err := iamClient.GetCredential(k)
		if err != nil {
			if e, ok := err.(error2.ApiErrorCode); ok {
				switch e {
				case error2.ErrForbiddenAccessKeyID,
					error2.ErrInvalidAccessKeyID,
					error2.ErrSuspendedAccessKeyID:
					IamCache.Delete(k)
				default:
					continue
				}
			}
			continue
		}
		entry := CacheEntry{
			CreateTime: time.Now(),
			Credential: credential,
		}
		IamCache.Store(k, entry)
	}
}

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

		go reexamineKeys(keysToExpire)
		time.Sleep(CACHE_CHECK_TIME)
	}
}

func InitializeIamCache() {
	go cacheInvalidator()
}
