package v2

import (
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/circuitbreak"
	"github.com/journeymidnight/yig/helper"
	"fmt"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"errors"
)

type AccessKeyItemList struct {
	Page int `json:"page"`
	Size int `json:"size"`
	Total int `json:"total_count"`
	Content []AccessKeyItem `json:"content"`
}

type AccessKeyItem struct {
	UserId       string `json:"user_id"`
	ProjectId    string `json:"project_id"`
	ProjectName  string `json:"project_name"`
	AccessKey    string `json:"access_key"`
	AccessSecret string `json:"access_secret"`
	Created      int64  `json:"create_at"`
	Expired      int64  `json:"expired_at"`
	Enabled      int    `json:"enabled"`
}


type Client struct {
	httpClient *circuitbreak.CircuitClient
}

func (a Client) GetKeysByUid (uid string) (credentials []common.Credential, err error) {
	if a.httpClient == nil {
		a.httpClient = circuitbreak.NewCircuitClient()
	}
	var slog = helper.Logger
	url := helper.CONFIG.IamEndpoint + "/v1/access/"
	request, _ := http.NewRequest("GET", url, nil)
	request.Header.Set("apikey", helper.CONFIG.IamKey)
	q := request.URL.Query()
	q.Add("project-id", uid)
	response, err := a.httpClient.Do(request)
	if err != nil {
		slog.Println(5, "GetKeysByUid send request failed", err)
		return credentials, err
	}

	if response.StatusCode != 200 {
		slog.Println(5, "GetKeysByUid to IAM failed return code = ", response.StatusCode)
		return credentials, fmt.Errorf("GetKeysByUid to IAM failed retcode = %d", response.StatusCode)
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		slog.Println(5, "GetKeysByUid ioutil.ReadAll failed")
		return credentials, fmt.Errorf("GetKeysByUid ioutil.ReadAll failed")
	}

	var resp AccessKeyItemList
	err = json.Unmarshal(body, &resp)
	if err != nil {
		return credentials, errors.New("Decode QueryResp failed")
	}
	for _, value := range resp.Content {
		credential := common.Credential{}
		credential.UserId = value.ProjectId
		credential.DisplayName = value.ProjectName
		credential.AccessKeyID = value.AccessKey
		credential.SecretAccessKey = value.AccessSecret
		credential.AllowOtherUserAccess = false
		credentials = append(credentials, credential)
	}
	return
}

func (a Client) GetCredential (accessKey string) (credential common.Credential, err error) {
	if a.httpClient == nil {
		a.httpClient = circuitbreak.NewCircuitClient()
	}
	var slog = helper.Logger
	url := helper.CONFIG.IamEndpoint + "/v1/access/" + accessKey
	request, _ := http.NewRequest("GET", url, nil)
	request.Header.Set("apikey", helper.CONFIG.IamKey)
	response, err := a.httpClient.Do(request)
	if err != nil {
		slog.Println(5, "GetCredential send request failed", err)
		return credential, err
	}

	if response.StatusCode != 200 {
		slog.Println(5, "GetCredential to IAM failed return code = ", response.StatusCode)
		return credential, fmt.Errorf("GetCredential to IAM failed retcode = %d", response.StatusCode)
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		slog.Println(5, "GetCredential ioutil.ReadAll failed")
		return credential, fmt.Errorf("GetCredential ioutil.ReadAll failed")
	}

	var resp AccessKeyItem
	err = json.Unmarshal(body, &resp)
	if err != nil {
		return credential, errors.New("Decode QueryResp failed")
	}
	credential.UserId = resp.ProjectId
	credential.DisplayName = resp.ProjectName
	credential.AccessKeyID = resp.AccessKey
	credential.SecretAccessKey = resp.AccessSecret
	credential.AllowOtherUserAccess = false
	return
}