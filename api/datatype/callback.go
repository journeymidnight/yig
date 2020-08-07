package datatype

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
)

const (
	MaxCallbackTimeout     = time.Second * 3
	Iso8601FormatTime      = "20060102T150405Z"
	CallBackUrl            = "X-Uos-Callback-Url"
	CallBackBody           = "X-Uos-Callback-Body"
	CallbackAuth           = "X-Uos-Callback-Auth"
	NeedCallbackAuth       = "1"
	CallBackLocationPrefix = "X-Uos-Callback-Custom-"
	Authorization          = "Authorization"
	ContentType            = "Content-Type"
	Date                   = "X-Uos-Date"
	CallbackAuthorization  = "UOS-CALLBACK-AUTH"
)

var (
	ImageSupportContentType = []string{
		"image/jpeg",
		"application/x-jpg",
		"image/png",
		"application/x-png",
		"image/gif",
	}
	ImageSupportSuffix = []string{
		".jpg",
		".jpeg",
		".png",
		".gif",
	}
	CallBackInfo = map[string]string{
		"bucket":     "${bucket}",
		"filename":   "${filename}",
		"etag":       "${etag}",
		"objectSize": "${objectSize}",
		"mimeType":   "${mimeType}",
		"createTime": "${createTime}",
		"height":     "${height}",
		"width":      "${width}",
	}
)

type CallBackInfos struct {
	BucketName string
	FileName   string
	Etag       string
	ObjectSize int64
	MimeType   string
	CreateTime uint64
	Height     int
	Width      int
}

type CallBackMessage struct {
	Url        string
	Auth       bool
	Location   map[string]string
	Info       map[string]string
	Credential common.Credential
}

func GetCallbackFromHeader(header http.Header) (isCallback bool, message CallBackMessage) {
	message = CallBackMessage{}
	url := header.Get(CallBackUrl)
	body := header.Get(CallBackBody)
	if url == "" || body == "" {
		return false, message
	}
	message.Url = url
	auth := header.Get(CallbackAuth)
	if auth == NeedCallbackAuth {
		message.Auth = true
	}
	info := make(map[string]string)
	infoSplits := strings.Split(body, "&")
	for _, split := range infoSplits {
		header := strings.Split(split, "=")
		for key, _ := range CallBackInfo {
			if key == header[0] {
				info[header[0]] = header[1]
			}
		}
	}
	message.Info = info
	location := map[string]string{}
	for head, value := range header {
		if strings.HasPrefix(head, CallBackLocationPrefix) {
			location[head] = value[0]
		}
	}
	message.Location = location
	return true, message
}

func GetCallbackFromForm(formValues map[string]string) (isCallback bool, message CallBackMessage) {
	message = CallBackMessage{}
	url := formValues[CallBackUrl]
	body := formValues[CallBackBody]
	if url == "" || body == "" {
		return false, message
	}
	message.Url = url
	auth := formValues[CallbackAuth]
	if auth == NeedCallbackAuth {
		message.Auth = true
	}
	info := make(map[string]string)
	infoSplits := strings.Split(body, "&")
	for _, split := range infoSplits {
		header := strings.Split(split, "=")
		for key, _ := range CallBackInfo {
			if key == header[0] {
				info[header[0]] = header[1]
			}
		}
	}
	message.Info = info
	location := map[string]string{}
	for head, value := range formValues {
		if strings.HasPrefix(head, CallBackLocationPrefix) {
			location[head] = value
		}
	}
	message.Location = location
	return true, message
}

func IsCallbackImageInfo(contentType string, objectName string) bool {
	for _, v := range ImageSupportContentType {
		if contentType == v {
			return true
		}
	}
	for _, v := range ImageSupportSuffix {
		if strings.Contains(objectName, v) {
			return true
		}
	}
	return false
}

func ValidCallbackImgInfo(infos map[string]string) bool {
	for key, _ := range infos {
		if key == "width" || key == "height" {
			return true
		}
	}
	return false
}

func ValidCallbackInfo(info map[string]string, noValidInfo CallBackInfos, location map[string]string) (map[string]string, error) {
	for key, value := range info {
		switch key {
		case "bucket":
			if value == CallBackInfo[key] {
				if noValidInfo.BucketName != "" {
					info[key] = noValidInfo.BucketName
				} else {
					info[key] = ""
				}
			}
			break
		case "filename":
			if value == CallBackInfo[key] {
				if noValidInfo.FileName != "" {
					info[key] = noValidInfo.FileName
				} else {
					info[key] = ""
				}
			}
			break
		case "etag":
			if value == CallBackInfo[key] {
				if noValidInfo.Etag != "" {
					info[key] = noValidInfo.Etag
				} else {
					info[key] = ""
				}
			}
			break
		case "objectSize":
			if value == CallBackInfo[key] {
				if noValidInfo.ObjectSize != 0 {
					info[key] = strconv.FormatInt(noValidInfo.ObjectSize, 10)
				} else {
					info[key] = ""
				}
			}
			break
		case "mimeType":
			if value == CallBackInfo[key] {
				if noValidInfo.MimeType != "" {
					info[key] = noValidInfo.MimeType
				} else {
					info[key] = ""
				}
			}
			break
		case "createTime":
			if value == CallBackInfo[key] {
				if noValidInfo.CreateTime != 0 {
					info[key] = strconv.FormatUint(noValidInfo.CreateTime, 10)
				} else {
					info[key] = ""
				}
			}
			break
		case "height":
			if value == CallBackInfo[key] {
				if noValidInfo.Height != 0 {
					info[key] = strconv.Itoa(noValidInfo.Height)
				} else {
					info[key] = ""
				}
			}
			break
		case "width":
			if value == CallBackInfo[key] {
				if noValidInfo.Width != 0 {
					info[key] = strconv.Itoa(noValidInfo.Width)
				} else {
					info[key] = ""
				}
			}
			break
		case "location":
			customs := strings.Split(value, ",")
			for _, custom := range customs {
				custom = strings.TrimPrefix(custom, "${")
				custom = strings.TrimSuffix(custom, "}")
				custom = strings.ToLower(custom)
				for k, v := range location {
					lowerK := strings.ToLower(k)
					if custom == lowerK {
						k = strings.TrimPrefix(k, CallBackLocationPrefix)
						k = strings.ToLower(k)
						if info[k] != "" {
							return nil, ErrValidCallBackInfo
						}
						info[k] = v
					}
				}
			}
			break
		}
	}
	return info, nil
}

func PostCallbackMessage(credential common.Credential, message CallBackMessage) (result string, err error) {
	client := http.Client{
		Timeout: MaxCallbackTimeout,
	}
	req, err := getPostRequest(credential, message)
	if err != nil {
		helper.Logger.Warn("Callback error with getPostRequest:", err)
		return "", ErrCallBackFailed
	}
	resp, err := client.Do(req)
	defer resp.Body.Close()
	if err != nil {
		if err == http.ErrHandlerTimeout {
			resp, err = client.Do(req)
			defer resp.Body.Close()
			if err != nil {
				if err == http.ErrHandlerTimeout {
					helper.Logger.Warn("Callback error with doRequest Timeout:", err)
					return "", ErrCallBackFailed
				}
				helper.Logger.Warn("Callback error with doRequest:", err)
				return "", ErrCallBackFailed
			}
		}
	}
	info, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		helper.Logger.Warn("Callback error with readResponse:", err)
		return "", ErrCallBackFailed
	}
	return string(info), nil
}

func getPostRequest(credential common.Credential, message CallBackMessage) (*http.Request, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	for k, v := range message.Info {
		if v != "" {
			if err := writer.WriteField(k, v); err != nil {
				return nil, err
			}
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	url := message.Url
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return nil, err
	}
	date := time.Now().UTC().Format(Iso8601FormatTime)
	if message.Auth {
		signature := getSignatureForCallback(credential, date)
		req.Header.Add(Authorization, signature)
	}
	req.Header.Add(ContentType, writer.FormDataContentType())
	req.Header.Add(Date, date)
	return req, nil
}

func getSignatureForCallback(credential common.Credential, date string) string {
	var key, data string
	key = credential.SecretAccessKey
	data = "POST\n" + date
	mac := hmac.New(sha1.New, []byte(key))
	mac.Write([]byte(data))
	signature := mac.Sum(nil)
	return CallbackAuthorization + " " + credential.AccessKeyID + ":" + base64.StdEncoding.EncodeToString(signature)
}

func GetImageInfoFromReader(reader io.Reader) (height, width int) {
	img, _, err := image.Decode(reader)
	if err != nil {
		return
	}
	bounds := img.Bounds()
	width = bounds.Max.X
	height = bounds.Max.Y
	return
}
