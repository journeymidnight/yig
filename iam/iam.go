// +build !debug

package iam

import (
	"encoding/json"
	"fmt"
	"git.letv.cn/yig/yig/helper"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
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
	Action     string   `json:"action"`
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



func GetCredential(accessKey string) (credential Credential, err error) {
	// should use a cache with timeout
	// TODO put iam addr to config
	var slog = helper.Logger
	var query Query
	var queryRetAll QueryRespAll
	client := &http.Client{}
	query.Action = "DescribeAccessKeys"
	query.AccessKeys[0] = accessKey

	b, err := json.Marshal(query)
	if err != nil {
		slog.Println("json err:", err)
		return Credential{}, err
	}

	request, _ := http.NewRequest("POST", helper.Cfg.IamEndpoint, strings.NewReader(string(b)))
	request.Header.Set("X-Le-Key", helper.Cfg.IamKey)
	request.Header.Set("X-Le-Secret", helper.Cfg.IamSecret)
	response, _ := client.Do(request)
	if response.StatusCode != 200 {
		slog.Println("Query to IAM failed as status != 200")
		return Credential{}, fmt.Errorf("Query to IAM failed as status != 200")
	}

	body, _ := ioutil.ReadAll(response.Body)
	slog.Println("iam:",helper.Cfg.IamEndpoint)
	slog.Println("request:",string(b))
	slog.Println("response:",string(body))
	dec := json.NewDecoder(strings.NewReader(string(body)))
	if err := dec.Decode(&queryRetAll); err != nil {
		slog.Println("Decode QueryHistoryResp failed")
		return Credential{}, fmt.Errorf("Decode QueryHistoryResp failed")
	}

	if queryRetAll.RetCode != 0 {
		slog.Println("Query to IAM failed as RetCode != 0")
		return Credential{}, fmt.Errorf("Query to IAM failed as RetCode != 0")
	}


	uid := queryRetAll.Data.AccessKeySet[0].ProjectId
	name := queryRetAll.Data.AccessKeySet[0].Name
	ak := queryRetAll.Data.AccessKeySet[0].AccessKey
	sk := queryRetAll.Data.AccessKeySet[0].AccessSecret

	slog.Println("ak:",ak)
	slog.Println("sk:",sk)
	return Credential{
		UserId:          uid,
		DisplayName:     name,
		AccessKeyID:     ak,
		SecretAccessKey: sk,
	}, nil // For test now
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
