package iam

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/journeymidnight/yig/circuitbreak"
	"github.com/journeymidnight/yig/helper"
	"io/ioutil"
	"net/http"
	"regexp"
	"time"
)

// IsValidSecretKey - validate secret key.
var IsValidSecretKey = regexp.MustCompile(`^.{8,40}$`)

// IsValidAccessKey - validate access key.
var IsValidAccessKey = regexp.MustCompile(`^[a-zA-Z0-9\\-\\.\\_\\~]{5,20}$`)

func GetCredential(accessKey string) (credential Credential, err error) {
	if helper.CONFIG.DebugMode == true {
		return Credential{
			UserId:          "hehehehe",
			DisplayName:     "hehehehe",
			AccessKeyID:     accessKey,
			SecretAccessKey: "hehehehe",
		}, nil // For test now
	}

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
		iamClient = circuitbreak.NewCircuitClient()
	}
	query.Action = "DescribeAccessKeys"
	query.AccessKeys = append(query.AccessKeys, accessKey)

	b, err := json.Marshal(query)
	if err != nil {
		return credential, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		select {
		case <-time.After(10 * time.Second):
			slog.Println(5, "send iam request timeout, over 10s")
		case <-ctx.Done():
			slog.Println(20, ctx.Err()) // prints "context deadline exceeded"
		}
	}()

	request, err := http.NewRequest("POST", helper.CONFIG.IamEndpoint, bytes.NewReader(b))
	if err != nil {
		return credential, err
	}

	request.Header.Set("X-Le-Key", helper.CONFIG.IamKey)
	request.Header.Set("X-Le-Secret", helper.CONFIG.IamSecret)
	request.Header.Set("content-type", "application/json")
	request = request.WithContext(ctx)
	response, err := iamClient.Do(request)
	if err != nil {
		return credential, err
	}
	if response.StatusCode != 200 {
		return credential, errors.New("Query to IAM failed as status != 200")
	}

	body, err := ioutil.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return credential, err
	}
	slog.Println(10, "iam:", helper.CONFIG.IamEndpoint)
	slog.Println(10, "request:", string(b))
	slog.Println(10, "response:", string(body))

	var queryRetAll QueryRespAll
	err = json.Unmarshal(body, &queryRetAll)
	if err != nil {
		return credential, errors.New("Decode QueryHistoryResp failed")
	}
	if queryRetAll.RetCode != 0 {
		return credential, errors.New("Query to IAM failed as RetCode != 0")
	}

	if queryRetAll.Data.Total > 0 {
		credential.UserId = queryRetAll.Data.AccessKeySet[0].ProjectId
		credential.DisplayName = queryRetAll.Data.AccessKeySet[0].Name
		credential.AccessKeyID = queryRetAll.Data.AccessKeySet[0].AccessKey
		credential.SecretAccessKey = queryRetAll.Data.AccessKeySet[0].AccessSecret
		iamCache.set(accessKey, credential)
		return credential, nil
	} else {
		return credential, errors.New("Access key does not exist")
	}

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
