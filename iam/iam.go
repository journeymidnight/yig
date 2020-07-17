package iam

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/cache"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/mods"
)

// IsValidSecretKey - validate secret key.
var IsValidSecretKey = regexp.MustCompile(`^.{8,40}$`)

// IsValidAccessKey - validate access key.
var IsValidAccessKey = regexp.MustCompile(`^[a-zA-Z0-9\\-\\.\\_\\~]{5,20}$`)

type IamClient interface {
	GetKeysByUid(string) ([]common.Credential, error)
	GetCredential(string) (common.Credential, error)
}

var iamClient IamClient
var accessKeyLockMap sync.Map // accessKey -> *sync.RWMutex

func InitializeIamClient(plugins map[string]*mods.YigPlugin) {
	cache.InitializeIamCache()
	//Search for iam plugins, if we have many iam plugins, always use the first
	for name, p := range plugins {
		if p.PluginType == mods.IAM_PLUGIN {
			c, err := p.Create(helper.CONFIG.Plugins[name].Args)
			if err != nil {
				message := fmt.Sprintf("Failed to initial iam plugin %s: err: %v",
					name, err)
				panic(message)
			}
			helper.Logger.Info("Use IAM plugin", name)
			iamClient = c.(IamClient)
			return
		}
	}
	panic("Failed to initialize any IAM plugin, quiting...\n")
}

func GetCredential(accessKey string) (credential common.Credential, err error) {
	l, _ := accessKeyLockMap.LoadOrStore(accessKey, new(sync.Mutex))
	aLock := l.(*sync.Mutex)
	aLock.Lock()
	defer aLock.Lock()
	c, hit := cache.IamCache.Load(accessKey)
	if !hit {
		credential, err = iamClient.GetCredential(accessKey)
		if err != nil {
			accessKeyLockMap.Delete(accessKey)
			return credential, err
		}
		entry := cache.CacheEntry{
			CreateTime: time.Now(),
			Credential: credential,
		}
		cache.IamCache.Store(accessKey, entry)
	} else {
		entry := c.(cache.CacheEntry)
		credential = entry.Credential
	}
	return credential, nil
}

func GetKeysByUid(uid string) (credentials []common.Credential, err error) {
	credentials, err = iamClient.GetKeysByUid(uid)
	return
}

func GetCredentialByUserId(userId string) (credential common.Credential, err error) {
	// should use a cache with timeout
	// TODO
	return common.Credential{
		UserId:          userId,
		DisplayName:     userId,
		AccessKeyID:     "hehehehe",
		SecretAccessKey: "hehehehe",
	}, nil // For test now
}
