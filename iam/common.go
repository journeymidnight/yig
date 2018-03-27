package iam

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/journeymidnight/yig/circuitbreak"
	"github.com/journeymidnight/yig/helper"
	"io/ioutil"
	"net/http"
	"strings"
)

// credential container for access and secret keys.
type Credential struct {
	UserId          string
	DisplayName     string
	AccessKeyID     string
	SecretAccessKey string
}

func (a Credential) String() string {
	accessStr := "AccessKey: " + a.AccessKeyID
	secretStr := "SecretKey: " + a.SecretAccessKey
	return accessStr + " " + secretStr + "\n"
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
	ProjectId  string   `json:"projectId,omitempty"`
	AccessKeys []string `json:"accessKeys,omitempty"`
	Offset     int      `json:"offset,omitempty"`
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

var iamClient *circuitbreak.CircuitClient

func GetKeysByUid(uid string) (keyslist []AccessKeyItem, err error) {

	var slog = helper.Logger
	var query Query
	if iamClient == nil {
		iamClient = circuitbreak.NewCircuitClient()
	}
	var offset int = 0
	var total int = 0
	query.Action = "DescribeAccessKeys"
	query.ProjectId = uid
	for {
		query.Offset = offset
		b, err := json.Marshal(query)
		if err != nil {
			slog.Println(5, "json err:", err)
			return keyslist, err
		}
		request, _ := http.NewRequest("POST", helper.CONFIG.IamEndpoint, strings.NewReader(string(b)))
		request.Header.Set("X-Le-Key", "key")
		request.Header.Set("X-Le-Secret", "secret")
		slog.Println(10, "replay request:", request, string(b))
		response, err := iamClient.Do(request)
		if err != nil {
			slog.Println(5, "replay histroy send request failed", err)
			return keyslist, err
		}

		if response.StatusCode != 200 {
			slog.Println(5, "QueryHistory to IAM failed as status != 200")
			return keyslist, fmt.Errorf("QueryHistory to IAM failed as status != 200")
		}

		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			slog.Println(5, "QueryHistory ioutil.ReadAll failed")
			return keyslist, fmt.Errorf("QueryHistory ioutil.ReadAll failed")
		}

		var queryRetAll QueryRespAll
		err = json.Unmarshal(body, &queryRetAll)
		if err != nil {
			return keyslist, errors.New("Decode QueryRespAll failed")
		}
		if queryRetAll.RetCode != 0 {
			return keyslist, errors.New("Query to IAM failed as RetCode != 0")
		}
		for _, value := range queryRetAll.Data.AccessKeySet {
			keyslist = append(keyslist, value)
		}
		total = queryRetAll.Data.Total
		count := len(queryRetAll.Data.AccessKeySet)
		if queryRetAll.Data.Offset+count < total {
			offset = queryRetAll.Data.Offset + count
		} else {
			break
		}
	}

	return keyslist, nil
}
