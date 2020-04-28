package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/journeymidnight/yig/crypto"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/mods"
	"io/ioutil"
	"net/http"
)

const (
	pluginName                    = "encryption_kms"
	MaxPageSize                   = 100
	NUMBEROFPALIN                 = 24
	KeyDescription                = "Use to encrypt sse-s3 plaintext"
	KMSResponseCodeOfTokenInvalid = "0x00000054"
)

type SDKMSConfig struct {
	Url             string
	KeyName         string
	AccessKey       string
	SecretAccessKey string
}

type SDKMS struct {
	Token  string
	KeyID  string
	Config *SDKMSConfig
}

type ResponseData struct {
	KeyID              string `json:"KeyId"`
	Plaintext          string `json:"Plaintext"`
	CiphertextBlob     string `json:"CiphertextBlob"`
	RequestId          string `json:"RequestId"`
	Code               string `json:"code"`
	Message            string `json:"message"`
	KeyRotationEnabled bool   `json:"keyRotationEnabled" `
}

type ListKeysResponse struct {
	Keys       []map[string]interface{} `json:"Keys"`
	PageNumber int                      `json:"PageNumber"`
	PageSize   int                      `json:"PageSize"`
	TotalCount int                      `json:"TotalCount"`
	Code       string                   `json:"code"`
	Message    string                   `json:"message"`
}

//The variable MUST be named as Exported.
//the code in yig-plugin will lookup this symbol
var Exported = mods.YigPlugin{
	Name:       pluginName,
	PluginType: mods.KMS_PLUGIN,
	Create:     GetKMSClient,
}

func GetKMSClient(config map[string]interface{}) (interface{}, error) {
	helper.Logger.Info("Get KMS plugin config:", config)

	if config["debug_mode"].(bool) == false {
		c, err := NewKMSConfig(config)
		if err != nil {
			panic("read kms err:" + err.Error())
		}
		kms, err := InitKMS(c)
		if err != nil {
			panic("Init kms err:" + err.Error())
		}
		return kms, nil
	}

	//connect with token
	kms := &SDKMS{
		Config: &SDKMSConfig{
			Url:             config["url"].(string),
			KeyName:         config["keyName"].(string),
			AccessKey:       "",
			SecretAccessKey: "",
		},
		Token: config["token"].(string),
	}
	if kms.Config.Url == "" {
		return nil, fmt.Errorf("Missing KMS url - %s is empty", "c.Url")
	}
	if kms.Token == "" {
		return nil, fmt.Errorf("Missing KMS Token - %s is empty", "c.Token")
	}
	if kms.Config.KeyName == "" {
		return nil, fmt.Errorf("Missing KMS keyName - %s is empty", "c.KeyName")
	}
	haveKey, err := kms.checkAndGetKMSKey()
	if err != nil {
		helper.Logger.Error("Check KMS key err:", err.Error())
		return nil, err
	}
	if !haveKey {
		err = kms.CreateKey(kms.Config.KeyName, KeyDescription)
		if err != nil {
			helper.Logger.Error("Create Key error:", err.Error())
			return nil, err
		}
	}
	KeyIsRotation, err := kms.GetKeyRotationStatus()
	if err != nil {
		return nil, err
	}
	if !KeyIsRotation {
		err = kms.EnableKeyRotation()
		if err != nil {
			helper.Logger.Error("Rotate Key error:", err.Error())
			return nil, err
		}
	}
	return kms, nil
}

func validateKMSConfig(c *SDKMSConfig) error {
	if c.Url == "" {
		return fmt.Errorf("Missing KMS url - %s is empty", "c.Url")
	}
	if c.KeyName == "" {
		return fmt.Errorf("Missing KMS keyName - %s is empty", "c.KeyName")
	}
	if c.AccessKey == "" {
		return fmt.Errorf("Missing KMS AccessKey - %s is empty", "c.AccessKey")
	}
	if c.SecretAccessKey == "" {
		return fmt.Errorf("Missing KMS SecretAccessKey - %s is empty", "c.SecretAccessKey")
	}
	return nil
}

//authenticate to KMS with AK、SK, and get a client access token
func getKMSAccessToken(url, ak, sk string) (token string, err error) {
	//TODO:
	return "", nil
}

func NewKMSConfig(config map[string]interface{}) (SDKMSConfig, error) {
	kmsConfig := SDKMSConfig{
		Url:             config["url"].(string),
		KeyName:         config["keyName"].(string),
		AccessKey:       config["accessKey"].(string),
		SecretAccessKey: config["secretAccessKey"].(string),
	}

	if err := validateKMSConfig(&kmsConfig); err != nil {
		return kmsConfig, err
	}
	return kmsConfig, nil
}

func InitKMS(kmsConfig SDKMSConfig) (interface{}, error) {
	kms := &SDKMS{
		Config: &kmsConfig,
	}
	var err error
	kms.Token, err = getKMSAccessToken(kms.Config.Url, kms.Config.AccessKey, kms.Config.SecretAccessKey)
	if err != nil {
		return nil, err
	}
	haveKey, err := kms.checkAndGetKMSKey()
	if err != nil {
		return nil, err
	}
	if !haveKey {
		err = kms.CreateKey(kms.Config.KeyName, KeyDescription)
		if err != nil {
			helper.Logger.Error("Create Key error:", err.Error())
			return nil, err
		}
	}
	KeyIsRotation, err := kms.GetKeyRotationStatus()
	if err != nil {
		return nil, err
	}
	if !KeyIsRotation {
		err = kms.EnableKeyRotation()
		if err != nil {
			helper.Logger.Error("Rotate Key error:", err.Error())
			return nil, err
		}
	}
	return kms, nil
}

func HTTPRequest(url, token string, date []byte) (int, []byte, error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	var client = &http.Client{Transport: tr}

	body := bytes.NewReader(date)
	request, err := http.NewRequest("POST", url, body)
	if err != nil {
		return 400, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Add("X-Auth-Token", token)

	res, err := client.Do(request)
	if err != nil {
		return res.StatusCode, nil, err
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return 400, nil, err
	}

	return res.StatusCode, data, nil
}

func (s *SDKMS) CreateKey(keyName, description string) error {
	requestData := map[string]interface{}{
		"AliasName":   keyName,
		"Description": description,
		"KeyUsage":    "ENCRYPT/DECRYPT",
		"Origin":      "KMS",
	}
	body, err := json.Marshal(requestData)
	if err != nil {
		return err
	}

	url := s.Config.Url + "/v1/CreateKey"

	statusCode, respDate, err := HTTPRequest(url, s.Token, body)
	if err != nil {
		helper.Logger.Error("StatusCode: ", statusCode, "Create key request err: ", err.Error())
		return err
	}
	var data ResponseData
	err = json.Unmarshal(respDate, &data)
	if err != nil {
		return err
	}
	if statusCode > 299 {
		return errors.New("error code: " + data.Code + " message: " + data.Message)
	}
	s.KeyID = data.KeyID
	return nil
}

func (s *SDKMS) EnableKeyRotation() error {
	requestData := map[string]interface{}{
		"KeyID": s.KeyID,
	}
	body, err := json.Marshal(requestData)
	if err != nil {
		return err
	}
	url := s.Config.Url + "/v1/EnableKeyRotation"

	statusCode, respDate, err := HTTPRequest(url, s.Token, body)
	if err != nil {
		helper.Logger.Error("StatusCode: ", statusCode, "Enable key rotation request err: ", err.Error())
		return err
	}
	var data ResponseData
	err = json.Unmarshal(respDate, &data)
	if err != nil {
		return err
	}
	if statusCode > 299 {
		return errors.New("error code: " + data.Code + " message: " + data.Message)
	}
	return nil
}

func (s *SDKMS) GetKeyRotationStatus() (bool, error) {
	requestData := map[string]interface{}{
		"KeyID": s.KeyID,
	}
	body, err := json.Marshal(requestData)
	if err != nil {
		return false, err
	}

	url := s.Config.Url + "/v1/GetKeyRotationStatus"

	statusCode, respDate, err := HTTPRequest(url, s.Token, body)
	if err != nil {
		helper.Logger.Error("StatusCode: ", statusCode, "Get key rotation status request err: ", err.Error())
		return false, err
	}
	var data ResponseData
	err = json.Unmarshal(respDate, &data)
	if err != nil {
		return false, err
	}
	if statusCode > 299 {
		return false, errors.New("error code: " + data.Code + " message: " + data.Message)
	}
	return data.KeyRotationEnabled, nil
}

func (s *SDKMS) ListKeys(pageNumber, pageSize int) (ListKeysResponse, error) {
	var data ListKeysResponse
	requestData := map[string]interface{}{
		"pageNumber": pageNumber,
		"pageSize":   pageSize,
	}
	body, err := json.Marshal(requestData)
	if err != nil {
		return data, err
	}
	url := s.Config.Url + "/v1/ListKeys"

	statusCode, respDate, err := HTTPRequest(url, s.Token, body)
	if err != nil {
		helper.Logger.Error("StatusCode: ", statusCode, "list keys request err: ", err.Error())
		return data, err
	}
	err = json.Unmarshal(respDate, &data)
	if err != nil {
		return data, err
	}
	if statusCode > 299 {
		return data, errors.New("error code: " + data.Code + " message: " + data.Message)
	}
	return data, nil
}

func (s *SDKMS) GenerateKey(keyName string, context crypto.Context) (key [32]byte, sealedKey []byte, err error) {
	requestData := map[string]interface{}{
		"KeyID":         s.KeyID,
		"KeySpec":       "AES_256",
		"NumberOfBytes": NUMBEROFPALIN,
	}
	body, err := json.Marshal(requestData)
	if err != nil {
		return key, sealedKey, err
	}
	url := s.Config.Url + "/v1/GenerateDataKey"

	statusCode, respDate, err := HTTPRequest(url, s.Token, body)
	if err != nil {
		helper.Logger.Error("StatusCode: ", statusCode, "Generate key request err: ", err.Error())
		return key, sealedKey, err
	}

	var data ResponseData
	err = json.Unmarshal(respDate, &data)
	if err != nil {
		return key, sealedKey, err
	}
	if statusCode > 299 {
		if data.Code == KMSResponseCodeOfTokenInvalid {
			s.Token, err = getKMSAccessToken(s.Config.Url, s.Config.AccessKey, s.Config.SecretAccessKey)
			if err != nil {
				return key, sealedKey, err
			}
		}
		return key, sealedKey, errors.New("error code: " + data.Code + " message: " + data.Message)
	}
	copy(key[:], []byte(data.Plaintext))
	return key, []byte(data.CiphertextBlob), nil
}

func (s *SDKMS) UnsealKey(keyName string, sealedKey []byte, context crypto.Context) (key [32]byte, err error) {
	requestData := map[string]interface{}{
		"CiphertextBlob": string(sealedKey[:]),
	}
	body, err := json.Marshal(requestData)
	if err != nil {
		return key, err
	}
	url := s.Config.Url + "/v1/Decrypt"

	statusCode, respDate, err := HTTPRequest(url, s.Token, body)
	if err != nil {
		helper.Logger.Error("StatusCode: ", statusCode, "Unseal key request err: ", err.Error())
		return key, err
	}
	var data ResponseData
	err = json.Unmarshal(respDate, &data)
	if err != nil {
		return key, err
	}
	if statusCode > 299 {
		if data.Code == KMSResponseCodeOfTokenInvalid {
			s.Token, err = getKMSAccessToken(s.Config.Url, s.Config.AccessKey, s.Config.SecretAccessKey)
			if err != nil {
				return key, err
			}
		}
		return key, errors.New("error code: " + data.Code + " message: " + data.Message)
	}
	copy(key[:], []byte(data.Plaintext))
	return key, nil
}

func (s *SDKMS) GetKeyID() string {
	return s.Config.KeyName
}

func (s *SDKMS) checkAndGetKMSKey() (bool, error) {
	pageNumber := 1

	data, err := s.ListKeys(pageNumber, MaxPageSize)
	if err != nil {
		return false, err
	}
	if data.TotalCount > 0 {
		if data.TotalCount > MaxPageSize {
			data, err = s.ListKeys(pageNumber+1, MaxPageSize)
			if err != nil {
				return false, err
			}
		}
		s.KeyID = data.Keys[len(data.Keys)-1]["keyId"].(string)
		return true, nil
	}
	return false, nil
}
