package iam

import (
	"regexp"

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

func InitializeIamClient(plugins map[string]*mods.YigPlugin) {
	//Search for iam plugins, if we have many iam plugins, always use the first
	for name, p := range plugins {
		if p.PluginType == mods.IAM_PLUGIN {
			c, err := p.Create(helper.CONFIG.Plugins[name].Args)
			if err != nil {
				helper.Logger.Fatalf(0, "failed to initial iam plugin %s: err: %v\n", name, err)
				return
			}
			helper.Logger.Printf(5, "Chosen IAM plugin %s..\n", name)
			iamClient = c.(IamClient)
			return
		}
	}
	helper.Logger.Fatalf(0, "failed to initial any iam plugin, quiting...\n")
	return
}

func GetCredential(accessKey string) (credential common.Credential, err error) {
	if cache.IamCache == nil {
		cache.InitializeIamCache()
	}

	credential, hit := cache.IamCache.Get(accessKey)
	if hit {
		return credential, nil
	}

	credential, err = iamClient.GetCredential(accessKey)
	if err != nil {
		return credential, err
	}
	cache.IamCache.Set(accessKey, credential)
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
