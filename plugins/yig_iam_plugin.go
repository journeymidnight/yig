package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/journeymidnight/yig/circuitbreak"
	"github.com/journeymidnight/yig/helper"
	_ "github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/mods"
	"net/http"
	"strings"
)

const pluginName = "yig_iam"

//The variable MUST be named as Exported.
//the code in yig-plugin will lookup this symbol
var Exported = mods.YigPlugin{
	Name:       pluginName,
	PluginType: mods.IAM_PLUGIN,
	Create:     GetIamClient,
}

type QueryRequest struct {
	Acl         string   `json:"acl,omitempty"`
	UserName    string   `json:"userName,omitempty"`
	UserId      string   `json:"userId,omitempty"`
	KeyName     string   `json:"keyName,omitempty"`
	ProjectId   string   `json:"projectId,omitempty"`
	ProjectName string   `json:"projectName,omitempty"`
	ProjectIds  []string `json:"projects,omitempty"`
	Password    string   `json:"password,omitempty"`
	Description string   `json:"description,omitempty"`
	DisplayName string   `json:"displayName,omitempty"`
	Type        string   `json:"type,omitempty"`
	Email       string   `json:"email,omitempty"`
	Token       string   `json:"token,omitempty"`
	AccessKey   string   `json:"accessKey,omitempty"`
	AccessKeys  []string `json:"accessKeys,omitempty"`
	Limit       int      `json:"limit,omitempty"`
	Offset      int      `json:"offset,omitempty"`
}

type AccessKeyItem struct {
	ProjectId    string `json:"projectId"`
	Name         string `json:"name"`
	AccessKey    string `json:"accessKey"`
	AccessSecret string `json:"accessSecret"`
	Acl          string `json:"acl"`
	Status       string `json:"status"`
	Updated      string `json:"updated"`
}

type FetchAccessKeysResp struct {
	AccessKeySet []AccessKeyItem `json:"accessKeySet"`
}

func GetIamClient(config map[string]interface{}) (interface{}, error) {

	helper.Logger.Info("Get plugin config:", config)

	c := YigIamClient{
		Endpoint:     config["EndPoint"].(string),
		ManageKey:    config["ManageKey"].(string),
		ManageSecret: config["ManageSecret"].(string),
	}

	return interface{}(c), nil
}

type YigIamClient struct {
	httpClient   *circuitbreak.CircuitClient
	Endpoint     string
	ManageKey    string
	ManageSecret string
}

func (c YigIamClient) GetKeysByUid(uid string) (credentials []common.Credential, err error) {
	if c.httpClient == nil {
		c.httpClient = circuitbreak.NewCircuitClientWithInsecureSSL()
	}
	var slog = helper.Logger

	url := c.Endpoint
	slog.Println(20, "Url of GetKeysByUid send request to IAM :", url)
	var query QueryRequest
	query.ProjectId = uid
	body, err := json.Marshal(query)
	if err != nil {
		slog.Println(5, "json err:", err)
		return credentials, err
	}

	request, _ := http.NewRequest("POST", url, strings.NewReader(string(body)))
	request.Header.Set("X-Le-Key", c.ManageKey)
	request.Header.Set("X-Le-Secret", c.ManageSecret)

	response, err := c.httpClient.Do(request)
	if err != nil {
		slog.Println(5, "GetKeysByUid send request failed", err)
		return credentials, err
	}
	var resp FetchAccessKeysResp
	err = helper.ReadJsonBody(response.Body, &resp)
	if err != nil {
		return credentials, errors.New("failed to read from IAM: " + err.Error())
	}
	slog.Println(20, "GetKeysByUid to IAM return status ", response.Status)
	if response.StatusCode != 200 {
		slog.Println(5, "GetKeysByUid to IAM failed return code = ", response.StatusCode)
		return credentials, fmt.Errorf("GetKeysByUid to IAM failed retcode = %d", response.StatusCode)
	}
	for _, value := range resp.AccessKeySet {
		credential := common.Credential{}
		credential.UserId = value.ProjectId
		credential.DisplayName = value.Name
		credential.AccessKeyID = value.AccessKey
		credential.SecretAccessKey = value.AccessSecret
		credential.AllowOtherUserAccess = false
		credentials = append(credentials, credential)
	}
	return
}

func (c YigIamClient) GetCredential(accessKey string) (credential common.Credential, err error) {
	if c.httpClient == nil {
		c.httpClient = circuitbreak.NewCircuitClientWithInsecureSSL()
	}
	var slog = helper.Logger

	url := c.Endpoint
	slog.Println(20, "Url of GetCredential send request to IAM :", url)
	var query QueryRequest
	query.AccessKey = accessKey
	body, err := json.Marshal(query)
	if err != nil {
		slog.Println(5, "json err:", err)
		return credential, err
	}

	request, _ := http.NewRequest("POST", url, strings.NewReader(string(body)))
	request.Header.Set("X-Le-Key", c.ManageKey)
	request.Header.Set("X-Le-Secret", c.ManageSecret)
	response, err := c.httpClient.Do(request)
	if err != nil {
		slog.Println(5, "GetCredential send request failed", err)
		return credential, err
	}
	var resp = new(FetchAccessKeysResp)
	err = helper.ReadJsonBody(response.Body, &resp)
	if err != nil {
		return credential, errors.New("failed to read from IAM: " + err.Error())
	}
	slog.Println(20, "GetCredential to IAM return status ", response.Status)
	if response.StatusCode != 200 {
		slog.Println(5, "GetCredential to IAM failed return code = ", response.StatusCode)
		return credential, fmt.Errorf("GetCredential to IAM failed retcode = %d", response.StatusCode)
	}

	if len(resp.AccessKeySet) == 1 {
		credential.UserId = resp.AccessKeySet[0].ProjectId
		credential.DisplayName = resp.AccessKeySet[0].Name
		credential.AccessKeyID = resp.AccessKeySet[0].AccessKey
		credential.SecretAccessKey = resp.AccessKeySet[0].AccessSecret
		credential.AllowOtherUserAccess = false
	} else {
		slog.Println(5, "GetCredential internal error retcode = %d", response.StatusCode)
		return credential, fmt.Errorf("GetCredential internal error retcode = %d", response.StatusCode)
	}

	return
}
