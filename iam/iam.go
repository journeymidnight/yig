package iam

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/journeymidnight/yig/helper"
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
var iamCache sync.Map

func InitializeIamClient(plugins map[string]*mods.YigPlugin) {
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
	c, hit := iamCache.Load(accessKey)
	if hit {
		return c.(common.Credential), nil
	}

	credential, err = iamClient.GetCredential(accessKey)
	if err != nil {
		return credential, err
	}

	iamCache.Store(accessKey, credential)
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
