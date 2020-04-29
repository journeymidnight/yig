package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/journeymidnight/yig/circuitbreak"
	"github.com/journeymidnight/yig/helper"
	_ "github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/mods"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

type AccessKeyItemList struct {
	Page    int             `json:"page"`
	Size    int             `json:"size"`
	Total   int             `json:"total_count"`
	Content []AccessKeyItem `json:"content"`
}

type AccessKeyItem struct {
	UserId       string `json:"userId"`
	ProjectId    string `json:"projectId"`
	ProjectName  string `json:"projectName"`
	AccessKey    string `json:"accessKey"`
	AccessSecret string `json:"accessSecret"`
	Created      string `json:"createAt"`
	Expired      string `json:"expiredAt"`
	Enabled      int    `json:"enabled"`
	Type         string `json:"type,omitempty"`
	Signature    string `json:"signature,omitempty"`
	Temp         string `json:"temp,omitempty"`
}

type QueryResp struct {
	Status       bool          `json:"status"`
	Auth         bool          `json:"auth"`
	Code         string        `json:"code"`
	AccessKeySet AccessKeyItem `json:"res,omitempty"`
	Message      string        `json:"msg"`
}

type IamV3Client struct {
	httpClient      *circuitbreak.CircuitClient
	IamUrl          string
	IamPath         string
	AccessKey       string
	SercetAccessKey string
}

const pluginName = "v3_iam"

var Exported = mods.YigPlugin{
	Name:       pluginName,
	PluginType: mods.IAM_PLUGIN,
	Create:     GetIamClient,
}

func GetIamClient(config map[string]interface{}) (interface{}, error) {

	helper.Logger.Println(10, "Get plugin config: %v\n", config)
	c := IamV3Client{
		IamUrl:          config["url"].(string),
		IamPath:         config["iamPath"].(string),
		AccessKey:       config["accessKey"].(string),
		SercetAccessKey: config["secretAccessKey"].(string),
	}
	return interface{}(c), nil
}

func (a IamV3Client) GetKeysByUid(uid string) (credentials []common.Credential, err error) {
	if a.httpClient == nil {
		a.httpClient = circuitbreak.NewCircuitClientWithInsecureSSL()
	}
	var slog = helper.Logger

	url := a.IamUrl + "/" + a.IamPath + "?" + "AccessKeyId=" + a.AccessKey
	slog.Println(10, "url is:", url)
	signUrl := SignWithRequestURL("GET", url, a.SercetAccessKey)
	slog.Println(20, "Url of GetKeysByUid send request to IAM :", signUrl)

	request, _ := http.NewRequest("GET", signUrl, nil)
	q := request.URL.Query()
	q.Add("project-id", uid)
	q.Add("page", "0")
	q.Add("size", "50")
	request.URL.RawQuery = q.Encode()
	response, err := a.httpClient.Do(request)
	if err != nil {
		slog.Println(5, "GetKeysByUid send request failed", err)
		return credentials, err
	}
	var resp AccessKeyItemList
	err = helper.ReadJsonBody(response.Body, &resp)
	if err != nil {
		return credentials, errors.New("failed to read from IAM: " + err.Error())
	}
	slog.Println(20, "GetKeysByUid to IAM return status ", response.Status)
	if response.StatusCode != 200 {
		slog.Println(5, "GetKeysByUid to IAM failed return code = ", response.StatusCode)
		return credentials, fmt.Errorf("GetKeysByUid to IAM failed retcode = %d", response.StatusCode)
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

func (a IamV3Client) GetCredential(accessKey string) (credential common.Credential, err error) {
	if a.httpClient == nil {
		a.httpClient = circuitbreak.NewCircuitClientWithInsecureSSL()
	}
	var slog = helper.Logger

	url := a.IamUrl + "/" + a.IamPath + "?" + "AccessKeyId=" + a.AccessKey + "&UserAccessKeyId=" + accessKey
	signUrl := SignWithRequestURL("GET", url, a.SercetAccessKey)
	slog.Println(20, "Url of GetCredential send request to IAM :", signUrl)

	request, _ := http.NewRequest("GET", signUrl, nil)
	response, err := a.httpClient.Do(request)
	if err != nil {
		slog.Println(5, "GetCredential send request failed", err)
		return credential, err
	}
	var resp = new(QueryResp)
	body := response.Body
	jsonBytes, err := ioutil.ReadAll(body)
	if err != nil {
		slog.Println(20, "Read IAM response err:", err)
		return credential, err
	}
	defer body.Close()
	s := string(jsonBytes)
	slog.Println(20, "Read IAM JSON:", s)
	err = json.Unmarshal(jsonBytes, resp)
	if err != nil {
		slog.Println(20, "Read IAM JSON err:", err)
		return credential, err
	}
	slog.Println(20, "GetCredential to IAM return status ", response.Status)
	if response.StatusCode != 200 {
		slog.Println(5, "GetCredential to IAM failed return code = ", response.StatusCode)
		return credential, fmt.Errorf("GetCredential to IAM failed retcode = %d", response.StatusCode)
	}

	value := resp.AccessKeySet
	credential.UserId = value.ProjectId
	credential.DisplayName = value.ProjectName
	credential.AccessKeyID = value.AccessKey
	credential.SecretAccessKey = value.AccessSecret
	credential.AllowOtherUserAccess = false

	return
}

type QuerySorter struct {
	Keys []string
	Vals []string
}

// Additional function for function SignHeader.
func newQuerySorter(m map[string]string) *QuerySorter {
	hs := &QuerySorter{
		Keys: make([]string, 0, len(m)),
		Vals: make([]string, 0, len(m)),
	}
	for k, v := range m {
		hs.Keys = append(hs.Keys, k)
		hs.Vals = append(hs.Vals, v)
	}
	return hs
}

//func SignWithRequestURL(httpMethod, requestUrl, secret string, queryParams) string {

//}
func SignWithRequestURL(httpMethod, requestUrl, secret string) string {
	u, err := url.Parse(requestUrl)
	if err != nil {
		panic(err)
	}
	temp := make(map[string]string)
	queryPair := strings.Split(u.RawQuery, "&")
	for _, pair := range queryPair {
		kvPair := strings.SplitN(pair, "=", 2)
		if len(kvPair) == 1 {
			temp[kvPair[0]] = percentEncode("")
		} else if len(kvPair) == 2 {
			temp[kvPair[0]] = percentEncode(kvPair[1])
		}
	}
	qs := newQuerySorter(temp)
	qs.Sort()
	canonicalizedQueryString := ""
	for i := range qs.Keys {
		canonicalizedQueryString += qs.Keys[i] + "=" + qs.Vals[i] + "&"
	}
	canonicalizedQueryString = canonicalizedQueryString[:len(canonicalizedQueryString)-1]
	stringToSign := httpMethod + "&" + percentEncode("/") + "&" +
		percentEncode(canonicalizedQueryString)
	h := hmac.New(func() hash.Hash { return sha1.New() }, []byte(secret+"&"))
	io.WriteString(h, stringToSign)
	signedStr := url.QueryEscape(base64.StdEncoding.EncodeToString(h.Sum(nil)))
	newQueryString, _ := url.QueryUnescape(canonicalizedQueryString)
	if strings.Contains(newQueryString, "#") {
		newQueryString = strings.Replace(newQueryString, "#", "%23", -1)
	}
	signedUrl := u.Scheme + "://" + u.Host + u.Path + "?" + newQueryString + "&Signature=" + signedStr
	return signedUrl
}

// Additional function for function SignHeader.
func (hs *QuerySorter) Sort() {
	sort.Sort(hs)
}

// Additional function for function SignHeader.
func (hs *QuerySorter) Len() int {
	return len(hs.Vals)
}

// Additional function for function SignHeader.
func (hs *QuerySorter) Less(i, j int) bool {
	return bytes.Compare([]byte(hs.Keys[i]), []byte(hs.Keys[j])) < 0
}

// Additional function for function SignHeader.
func (hs *QuerySorter) Swap(i, j int) {
	hs.Vals[i], hs.Vals[j] = hs.Vals[j], hs.Vals[i]
	hs.Keys[i], hs.Keys[j] = hs.Keys[j], hs.Keys[i]
}

func percentEncode(value string) string {
	if strings.Contains(value, "+") || strings.Contains(value, " ") {
		value = strings.Replace(value, "+", "%20", -1)
		value = strings.Replace(value, " ", "%20", -1)
		return value
	}
	return url.QueryEscape(value)
}
