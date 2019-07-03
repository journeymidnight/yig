package main

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
)

type DebugIamClient struct {
	IamUrl string
}

func (d DebugIamClient) GetKeysByUid(uid string) (c []common.Credential, err error) {
	return
}

func (d DebugIamClient) GetCredential(accessKey string) (c common.Credential, err error) {
	return common.Credential{
		UserId:          "hehehehe",
		DisplayName:     "hehehehe",
		AccessKeyID:     accessKey,
		SecretAccessKey: "hehehehe",
	}, nil // For test now
}

func GetIamClient() (c iam.IamClient, err error) {
	// Get config data
	data := helper.CONFIG.Plugins[iam.IamPluginName].Data
	helper.Logger.Println(20, "Get plugin data:", data)
	c = DebugIamClient{
		IamUrl: data["url"].(string),
	}
	return
}
