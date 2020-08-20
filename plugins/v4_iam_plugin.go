package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/journeymidnight/yig/api/datatype/policy"
	"github.com/journeymidnight/yig/circuitbreak"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	_ "github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/mods"
)

const RootPolicy = `{"Statement":[{"Principal":"*","Action":"s3:FullContorl","Effect":"Allow","Resource":"arn:aws:s3:::*"}],"Version":"2012-10-17"}`

type AccessKeyItemList struct {
	Page    int             `json:"page"`
	Size    int             `json:"size"`
	Total   int             `json:"total_count"`
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
	Type         string `json:"type,omitempty"`
	Signature    string `json:"signature,omitempty"`
	Temp         string `json:"temp,omitempty"`
}

type AccessInfo struct {
	UserId       string `json:"user_id"`
	ProjectId    string `json:"project_id"`
	ProjectName  string `json:"project_name"`
	AccessKey    string `json:"access_key"`
	AccessSecret string `json:"access_secret"`
	Created      int64  `json:"create_at"`
	Expired      int64  `json:"expired_at"`
	Enabled      int    `json:"enabled"`
	Type         string `json:"type,omitempty"`
}

type RamPolicy struct {
	Id        string `json:"id"`
	PolicyDoc string `json:"policy_doc"`
}

type User struct {
	ProjectId string `json:"default_project_uuid"`
	Enabled   int    `json:"enabled"`
	Id        string `json:"id"`
	Name      string `json:"name"`
	Type      string `json:"type,omitempty"`
}

type Res struct {
	Access     AccessInfo  `json:"access_info"`
	ChildUser  User        `json:"child_user,omitempty"`
	RootUser   User        `json:"root_user"`
	PolicyList []RamPolicy `json:"policy_list"`
}

type QueryResp struct {
	Status  bool   `json:"status"`
	Auth    bool   `json:"auth"`
	Code    string `json:"code"`
	Res     Res    `json:"res"`
	Message string `json:"msg"`
}

type IamV4Client struct {
	httpClient      *circuitbreak.CircuitClient
	IamUrl          string
	IamPath         string
	AccessKey       string
	SecretAccessKey string
	LogDelivererAK  string
	LogDelivererSK  string
}

const pluginName = "v4_iam"

var Exported = mods.YigPlugin{
	Name:       pluginName,
	PluginType: mods.IAM_PLUGIN,
	Create:     GetIamClient,
}

func GetIamClient(config map[string]interface{}) (interface{}, error) {
	helper.Logger.Info("Get plugin config: %v\n", config)
	c := IamV4Client{
		IamUrl:          config["url"].(string),
		IamPath:         config["iamPath"].(string),
		AccessKey:       config["accessKey"].(string),
		SecretAccessKey: config["secretAccessKey"].(string),
		LogDelivererAK:  config["logDelivererAK"].(string),
		LogDelivererSK:  config["logDelivererSK"].(string),
	}
	return interface{}(c), nil
}

func (a IamV4Client) GetKeysByUid(uid string) (credentials []common.Credential, err error) {
	// if a.httpClient == nil {
	// 	a.httpClient = circuitbreak.NewCircuitClientWithInsecureSSL()
	// }
	// var slog = helper.Logger

	// url := a.IamUrl + "/" + a.IamPath + "?" + "AccessKeyId=" + a.AccessKey
	// slog.Info("url is:", url)
	// signUrl := SignWithRequestURL("GET", url, a.SecretAccessKey)
	// slog.Info("Url of GetKeysByUid send request to IAM :", signUrl)

	// request, _ := http.NewRequest("GET", signUrl, nil)
	// q := request.URL.Query()
	// q.Add("project-id", uid)
	// q.Add("page", "0")
	// q.Add("size", "50")
	// request.URL.RawQuery = q.Encode()
	// response, err := a.httpClient.Do(request)
	// if err != nil {
	// 	slog.Error("GetKeysByUid send request failed", err)
	// 	return credentials, err
	// }
	// var resp AccessKeyItemList
	// err = helper.ReadJsonBody(response.Body, &resp)
	// if err != nil {
	// 	return credentials, errors.New("failed to read from IAM: " + err.Error())
	// }
	// slog.Info("GetKeysByUid to IAM return status ", response.Status)
	// if response.StatusCode != 200 {
	// 	slog.Warn("GetKeysByUid to IAM failed return code = ", response.StatusCode)
	// 	return credentials, fmt.Errorf("GetKeysByUid to IAM failed retcode = %d", response.StatusCode)
	// }
	// for _, value := range resp.Content {
	// 	credential := common.Credential{}
	// 	// credential.UserId = value.ProjectId
	// 	credential.DisplayName = value.ProjectName
	// 	credential.AccessKeyID = value.AccessKey
	// 	credential.SecretAccessKey = value.AccessSecret
	// 	credential.AllowOtherUserAccess = false
	// 	credentials = append(credentials, credential)
	// }
	return
}

func (a IamV4Client) GetCredential(accessKey string) (credential common.Credential, err error) {
	rootPolicy := policy.Policy{}
	err = json.Unmarshal([]byte(RootPolicy), &rootPolicy)
	if err != nil {
		return
	}
	// HACK: for put log temporary
	if accessKey == a.LogDelivererAK {
		credential.ExternUserId = "JustForPutLog"
		credential.AccessKeyID = accessKey
		credential.SecretAccessKey = a.LogDelivererSK
		credential.AllowOtherUserAccess = false
	} else {
		if a.httpClient == nil {
			a.httpClient = circuitbreak.NewCircuitClientWithInsecureSSL()
		}
		var slog = helper.Logger

		url := a.IamUrl + "/" + a.IamPath + "?" + "AccessKeyId=" + a.AccessKey + "&UserAccessKeyId=" + accessKey
		signUrl := SignWithRequestURL("GET", url, a.SecretAccessKey)
		slog.Info("Url of GetCredential send request to IAM :", signUrl)

		request, _ := http.NewRequest("GET", signUrl, nil)
		response, err := a.httpClient.Do(request)
		if err != nil {
			slog.Error("GetCredential send request failed", err)
			return credential, ErrInternalError
		}
		slog.Info("GetCredential to IAM return status ", response.Status)
		if response.StatusCode != 200 {
			slog.Error("GetCredential to IAM failed return code = ", response.StatusCode, " UCO Exception!!")
			return credential, ErrInternalError
		}
		var resp = new(QueryResp)
		body := response.Body
		jsonBytes, err := ioutil.ReadAll(body)
		if err != nil {
			slog.Error("Read IAM response err:", err)
			return credential, ErrInternalError
		}
		defer body.Close()
		s := string(jsonBytes)
		slog.Info("Read IAM JSON:", s)
		err = json.Unmarshal(jsonBytes, resp)
		if err != nil {
			slog.Error(" IAM JSON:", s, "Read IAM JSON err:", err)
			return credential, ErrInternalError
		}
		switch resp.Code {
		case "0":
			// normal
			break
		case "1":
			slog.Error("Get Error from OP")
			return credential, ErrInternalError
		case "3005":
			// AK and SK have forbidden
			return credential, ErrForbiddenAccessKeyID
		case "3006":
			// The user has not signed the agreement
			return credential, ErrInvalidAccessKeyID
		case "3007":
			// Arrears
			return credential, ErrSuspendedAccessKeyID
		case "3009":
			// AK invalid
			return credential, ErrInvalidAccessKeyID
		default:
			slog.Error("Get Error from UCO")
			return credential, ErrInternalError
		}

		value := resp.Res.Access
		credential.AccessKeyID = value.AccessKey
		credential.SecretAccessKey = value.AccessSecret
		credential.ExternUserId = value.UserId
		credential.ExternRootId = resp.Res.RootUser.Id
		if credential.ExternUserId == credential.ExternRootId {
			credential.DisplayName = resp.Res.RootUser.Name
			credential.Policy = &rootPolicy
		} else {
			credential.DisplayName = resp.Res.ChildUser.Name
			for _, p := range resp.Res.PolicyList {
				pp := policy.Policy{}
				err = json.Unmarshal([]byte(p.PolicyDoc), &pp)
				if err != nil {
					slog.Info("GetCredential decode policy_doc failed ", err)
					continue
				}
				if pp.Version != "2012-10-17" {
					continue
				}
				credential.Policy = &pp
				break
			}
		}
		credential.ExternRootName = resp.Res.RootUser.Name
		credential.AllowOtherUserAccess = false
		slog.Info("V4 credential", credential)
	}

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
