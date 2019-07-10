package main

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/mods"
)


const pluginName = "dummy_iam"

//The variable MUST be named as Exported.
//the code in yig-plugin will lookup this symbol
var Exported = mods.YigPlugin{
	Name:       pluginName,
	PluginType: mods.IAM_PLUGIN,
	Create:  GetIamClient,
}


func GetIamClient(config map[string]interface{}) (interface{}, error) {

	helper.Logger.Printf(10, "Get plugin config: %v\n", config)

	c := DebugIamClient{
		IamUrl: config["url"].(string),
	}

	return interface{}(c), nil
}

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

