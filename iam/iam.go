package iam

import (
	"plugin"
	"regexp"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/cache"
	"github.com/journeymidnight/yig/iam/common"
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

const IamPluginName = "iam"

func InitializeIamClient() {
	p, ok := helper.CONFIG.Plugins[IamPluginName]
	if !ok {
		helper.Logger.Println(20, "No iam plug-in settings in yig config.")
		panic("No iam plug-in settings in yig config.")
	}
	plug, err := plugin.Open(p.Path)
	if err != nil {
		helper.Logger.Println(20, "Invalid iam plug-in path. err:", err)
		panic("Invalid iam plug-in path")
	}
	iamPlug, err := plug.Lookup("GetIamClient")
	if err != nil {
		helper.Logger.Println(20, "Cannot find exported func 'GetIamClient' from iam plug-in. err:", err)
		panic("Cannot find exported func 'GetIamClient' from iam plug-in. err:" + err.Error())
	}
	c, err := iamPlug.(func() (IamClient, error))()
	if err != nil {
		helper.Logger.Println(20, "GetIamClient err:", err)
		panic("GetIamClient err:" + err.Error())
	}
	iamClient = c
}

func GetCredential(accessKey string) (credential common.Credential, err error) {
	if helper.CONFIG.DebugMode == true {
		return common.Credential{
			UserId:          "hehehehe",
			DisplayName:     "hehehehe",
			AccessKeyID:     accessKey,
			SecretAccessKey: "hehehehe",
		}, nil // For test now
	}

	if cache.IamCache == nil {
		cache.InitializeIamCache()
	}
	credential, hit := cache.IamCache.Get(accessKey)
	if hit {
		return credential, nil
	}

	if iamClient == nil {
		InitializeIamClient()
	}

	credential, err = iamClient.GetCredential(accessKey)
	if err != nil {
		return credential, err
	}
	cache.IamCache.Set(accessKey, credential)
	return credential, nil

}

func GetKeysByUid(uid string) (credentials []common.Credential, err error) {
	if helper.CONFIG.DebugMode == true {
		return
	}
	if iamClient == nil {
		InitializeIamClient()
	}
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
