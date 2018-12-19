package iam

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/cache"
	"github.com/journeymidnight/yig/iam/client/v1"
	"github.com/journeymidnight/yig/iam/client/v2"
	"github.com/journeymidnight/yig/iam/common"
	"regexp"
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

func initializeIamClient() {
	switch helper.CONFIG.IamVersion {
	case "v1":
		iamClient = v1.Client{}
	case "v2":
		iamClient = v2.Client{}
	default:
		panic("Unsupport iam version")
	}
	return
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
		initializeIamClient()
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
		initializeIamClient()
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
