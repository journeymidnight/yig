package main

import (
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
)

type IamClient struct{}

func (d IamClient) GetKeysByUid(uid string) (c []common.Credential, err error) {
	return
}

func (d IamClient) GetCredential(accessKey string) (c common.Credential, err error) {
	return common.Credential{
		UserId:          "hehehehe",
		DisplayName:     "hehehehe",
		AccessKeyID:     accessKey,
		SecretAccessKey: "hehehehe",
	}, nil // For test now
}

func GetIamClient() (c iam.IamClient, err error) {
	c = IamClient{}
	return
}
