package main

import (
	"encoding/json"

	"github.com/journeymidnight/yig/api/datatype/policy"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/mods"
)

const pluginName = "dummy_iam"

const ramPolicy = `{"Statement":[{"Principal":"*","Action":"s3:FullControl","Effect":"Allow","Resource":"arn:aws:s3:::*"}],"Version":"2012-10-17"}`

//The variable MUST be named as Exported.
//the code in yig-plugin will lookup this symbol
var Exported = mods.YigPlugin{
	Name:       pluginName,
	PluginType: mods.IAM_PLUGIN,
	Create:     GetIamClient,
}

func GetIamClient(config map[string]interface{}) (interface{}, error) {

	helper.Logger.Info("Get plugin config:", config)

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
	policy := policy.Policy{}
	err = json.Unmarshal([]byte(ramPolicy), &policy)
	if err != nil {
		helper.Logger.Error("GetCredential decode policy error:", err)
		return
	}
	helper.Logger.Info("GetCredential decode policy:", policy)
	if accessKey == "hehehehe" {
		return common.Credential{
			ExternUserId:    "idforhe",
			ExternRootId:    "idforhe",
			DisplayName:     "hehehehe",
			ExternRootName:  "hehehehe",
			AccessKeyID:     accessKey,
			SecretAccessKey: "hehehehe",
			Policy:          &policy,
		}, nil // For test now
	} else {
		// sub user of hehehehe
		return common.Credential{
			ExternUserId:    "idforha",
			ExternRootId:    "idforhe",
			DisplayName:     "hahahaha",
			ExternRootName:  "hehehehe",
			AccessKeyID:     accessKey,
			SecretAccessKey: "hahahaha",
		}, nil // For test now
	}
}
