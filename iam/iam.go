// +build !debug

package iam

import (
	"encoding/json"
	"fmt"
	"git.letv.cn/yig/yig/helper"
	"io/ioutil"
	"net/http"
	"regexp"
	"bytes"
)

// credential container for access and secret keys.
type Credential struct {
	UserId          string
	DisplayName     string
	AccessKeyID     string
	SecretAccessKey string
}

type AccessKeyItem struct {
	ProjectId    string `json:"projectId"`
	Name         string `json:"name"`
	AccessKey    string `json:"accessKey"`
	AccessSecret string `json:"accessSecret"`
	Status       string `json:"status"`
	Updated      string `json:"updated"`
}

type Query struct {
	Action string `json:"action"`
	//	ProjectId  string   `json:"projectId"`
	AccessKeys [1]string `json:"accessKeys"`
	//	Limit      int      `json:"limit"`
}

type QueryResp struct {
	Limit        int             `json:"limit"`
	Total        int             `json:"total"`
	Offset       int             `json:"offset"`
	AccessKeySet []AccessKeyItem `json:"accessKeySet"`
}

type QueryRespAll struct {
	Message string    `json:"message"`
	Data    QueryResp `json:"data"`
	RetCode int       `json:"retCode"`
}

// stringer colorized access keys.
func (a Credential) String() string {
	accessStr := "AccessKey: " + a.AccessKeyID
	secretStr := "SecretKey: " + a.SecretAccessKey
	return fmt.Sprint(accessStr + "  " + secretStr)
}

// IsValidSecretKey - validate secret key.
var IsValidSecretKey = regexp.MustCompile(`^.{8,40}$`)

// IsValidAccessKey - validate access key.
var IsValidAccessKey = regexp.MustCompile(`^[a-zA-Z0-9\\-\\.\\_\\~]{5,20}$`)

var iamClient *http.Client

func GetCredential(accessKey string) (credential Credential, err error) {
	if iamCache == nil {
		initializeIamCache()
	}
	credential, hit := iamCache.get(accessKey)
	if hit {
		return credential, nil
	}

	var slog = helper.Logger
	var query Query
	if iamClient == nil {
		iamClient = new(http.Client)
	}
	query.Action = "DescribeAccessKeys"
	query.AccessKeys[0] = accessKey

	b, err := json.Marshal(query)
	if err != nil {
		return credential, err
	}

	request, err := http.NewRequest("POST", helper.CONFIG.IamEndpoint, bytes.NewReader(b))
	if err != nil {
		return credential, err
	}
	request.Header.Set("X-Le-Key", helper.CONFIG.IamKey)
	request.Header.Set("X-Le-Secret", helper.CONFIG.IamSecret)
	response, err := iamClient.Do(request)
	if err != nil {
		return credential, err
	}
	if response.StatusCode != 200 {
		return credential, fmt.Errorf("Query to IAM failed as status != 200")
	}

	body, err := ioutil.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return credential, err
	}
	slog.Println("iam:", helper.CONFIG.IamEndpoint)
	slog.Println("request:", string(b))
	slog.Println("response:", string(body))

	var queryRetAll QueryRespAll
	err = json.Unmarshal(body, &queryRetAll)
	if err != nil {
		return credential, fmt.Errorf("Decode QueryHistoryResp failed")
	}
	if queryRetAll.RetCode != 0 {
		return credential, fmt.Errorf("Query to IAM failed as RetCode != 0")
	}

	credential.UserId = queryRetAll.Data.AccessKeySet[0].ProjectId
	credential.DisplayName = queryRetAll.Data.AccessKeySet[0].Name
	credential.AccessKeyID = queryRetAll.Data.AccessKeySet[0].AccessKey
	credential.SecretAccessKey = queryRetAll.Data.AccessKeySet[0].AccessSecret

	iamCache.set(accessKey, credential)
	return credential, nil
}

func GetCredentialByUserId(userId string) (credential Credential, err error) {
	// should use a cache with timeout
	// TODO
	return Credential{
		UserId:          userId,
		DisplayName:     userId,
		AccessKeyID:     "hehehehe",
		SecretAccessKey: "hehehehe",
	}, nil // For test now
}
