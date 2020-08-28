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
	"net/textproto"
	"net/url"
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
	CallBackLocationPrefix = "${x-uos-callback-customize-"
	Authorization          = "Authorization"
	ContentType            = "Content-Type"
	Date                   = "X-Uos-Date"
	CallbackAuthorization  = "UOS-CALLBACK-AUTH"
	MaxBodySize            = 1 << 20 // 1M
)

var (
	// Supported image format
	ImageSupportContentType = []string{
		"image/jpeg",
		"application/x-jpg",
		"image/png",
		"application/x-png",
		"image/gif",
	}
	// Supported image suffix format
	ImageSupportSuffix = []string{
		".jpg",
		".jpeg",
		".png",
		".gif",
	}
	// Parameters that can be used by magic variables
	CallBackInfo = []string{
		"${bucket}",
		"${filename}",
		"${etag}",
		"${objectSize}",
		"${mimeType}",
		"${createTime}",
	}
	CallBackInfoImg = []string{
		"${image.height}",
		"${image.width}",
		"${image.format}",
	}
)

type CallBackMagicInfos struct {
	BucketName string
	FileName   string
	VersionId  string
	Etag       string
	ObjectSize int64
	MimeType   string
	CreateTime uint64
	Height     int
	Width      int
	Format     string
}

type (
	// Used to store magic variable parameters
	MagicParam map[string]string
	// Used to store user-defined parameters
	Location map[string]string
	// Used to store user-defined constant parameters
	Constant map[string]string
)

type CallBackMessage struct {
	Url        string
	Auth       bool
	Magic      MagicParam
	Location   Location
	Constant   Constant
	Infos      map[string]string // Record the last parameters to be put into the POST request
	Credential common.Credential
}

func (c *CallBackMessage) IsCallbackImgNeedParse(contentType string, objectName string) (bool, error) {
	for _, info := range c.Magic {
		for _, key := range CallBackInfoImg {
			if info == key {
				return hasCallbackImageCanParse(contentType, objectName)
			}
		}
	}
	return false, nil
}

func GetCallbackFromHeader(header http.Header) (isCallback bool, message CallBackMessage, err error) {
	message = CallBackMessage{}
	bodyUrl := header.Get(CallBackUrl)
	body := header.Get(CallBackBody)
	if bodyUrl == "" && body == "" {
		return false, message, nil
	} else if (bodyUrl == "" && body != "") || (bodyUrl != "" && body == "") {
		return false, message, ErrInvalidCallbackParameter
	}
	message.Url = bodyUrl
	auth := header.Get(CallbackAuth)
	if auth == NeedCallbackAuth {
		message.Auth = true
	}
	message.Magic = make(map[string]string)
	message.Location = make(map[string]string)
	message.Constant = make(map[string]string)
	info, err := url.ParseQuery(body)
	if err != nil {
		return false, message, ErrInvalidCallbackBodyParameter
	}
	for k, v := range info {
		// Parse magic variables
		for _, key := range CallBackInfo {
			if key == v[0] {
				message.Magic[k] = v[0]
				break
			}
		}
		if _, ok := message.Magic[k]; ok {
			continue
		}
		for _, key := range CallBackInfoImg {
			if key == v[0] {
				message.Magic[k] = v[0]
				break
			}
		}
		if _, ok := message.Magic[k]; ok {
			continue
		}
		// Parse custom variables
		if strings.HasPrefix(strings.ToLower(v[0]), CallBackLocationPrefix) {
			markWithoutPrefix := strings.TrimPrefix(v[0], "${")
			mark := strings.TrimSuffix(markWithoutPrefix, "}")
			if mark == markWithoutPrefix {
				return false, message, ErrInvalidCallbackBodyParameter
			}
			mark = textproto.CanonicalMIMEHeaderKey(mark)
			message.Location[k] = header.Get(mark)
			continue
		}
		// Parse user-defined constant parameters
		message.Constant[k] = v[0]
	}
	return true, message, nil
}

func GetCallbackFromForm(formValues map[string]string) (isCallback bool, message CallBackMessage, err error) {
	message = CallBackMessage{}
	bodyUrl := formValues[CallBackUrl]
	body := formValues[CallBackBody]
	if bodyUrl == "" && body == "" {
		return false, message, nil
	} else if (bodyUrl == "" && body != "") || (bodyUrl != "" && body == "") {
		return false, message, ErrInvalidCallbackParameter
	}
	message.Url = bodyUrl
	auth := formValues[CallbackAuth]
	if auth == NeedCallbackAuth {
		message.Auth = true
	}
	message.Magic = make(map[string]string)
	message.Location = make(map[string]string)
	message.Constant = make(map[string]string)
	decodeBody, err := base64.StdEncoding.DecodeString(body)
	if err != nil {
		return false, message, ErrInvalidCallbackParameter
	}
	body = string(decodeBody)
	info, err := url.ParseQuery(body)
	if err != nil {
		return false, message, ErrInvalidCallbackBodyParameter
	}
	for k, v := range info {
		// Parse magic variables
		for _, key := range CallBackInfo {
			if key == v[0] {
				message.Magic[k] = v[0]
				break
			}
		}
		if _, ok := message.Magic[k]; ok {
			continue
		}
		for _, key := range CallBackInfoImg {
			if key == v[0] {
				message.Magic[k] = v[0]
				break
			}
		}
		if _, ok := message.Magic[k]; ok {
			continue
		}
		// Parse custom variables
		if strings.HasPrefix(strings.ToLower(v[0]), CallBackLocationPrefix) {
			markWithoutPrefix := strings.TrimPrefix(strings.ToLower(v[0]), "${")
			mark := strings.TrimSuffix(markWithoutPrefix, "}")
			if mark == markWithoutPrefix {
				return false, message, ErrInvalidCallbackBodyParameter
			}
			for formKey, formValue := range formValues {
				formKey = strings.ToLower(formKey)
				if formKey == mark {
					message.Location[k] = formValue
					break
				}
			}
			continue
		}
		// Parse user-defined constant parameters
		message.Constant[k] = v[0]
	}
	return true, message, nil
}

func hasCallbackImageCanParse(contentType string, objectName string) (bool, error) {
	for _, v := range ImageSupportContentType {
		if contentType == v {
			return true, nil
		}
	}
	for _, v := range ImageSupportSuffix {
		if strings.Contains(objectName, v) {
			return true, nil
		}
	}
	return false, ErrInvalidCallbackMagicImageType
}

func ParseCallbackInfos(magicInfo CallBackMagicInfos, message CallBackMessage) (messageFinished CallBackMessage, err error) {
	info := make(map[string]string)
	// Replace the corresponding magic variable with the object parameter
	for key, value := range message.Magic {
		switch value {
		case "${bucket}":
			if magicInfo.BucketName != "" {
				info[key] = magicInfo.BucketName
			} else {
				return message, ErrGetCallbackMagicParameter
			}
			break
		case "${filename}":
			if magicInfo.FileName != "" {
				info[key] = magicInfo.FileName
			} else {
				return message, ErrGetCallbackMagicParameter
			}
			break
		case "${etag}":
			if magicInfo.Etag != "" {
				info[key] = magicInfo.Etag
			} else {
				return message, ErrGetCallbackMagicParameter
			}
			break
		case "${objectSize}":
			if magicInfo.ObjectSize > 0 {
				info[key] = strconv.FormatInt(magicInfo.ObjectSize, 10)
			} else {
				return message, ErrGetCallbackMagicParameter
			}
			break
		case "${mimeType}":
			if magicInfo.MimeType != "" {
				info[key] = magicInfo.MimeType
			} else {
				return message, ErrGetCallbackMagicParameter
			}
			break
		case "${createTime}":
			if magicInfo.CreateTime > 0 {
				info[key] = strconv.FormatUint(magicInfo.CreateTime, 10)
			} else {
				return message, ErrGetCallbackMagicParameter
			}
			break
		case "${image.height}":
			if magicInfo.Height > 0 {
				info[key] = strconv.Itoa(magicInfo.Height)
			} else {
				return message, ErrGetCallbackMagicParameter
			}
			break
		case "${image.width}":
			if magicInfo.Width > 0 {
				info[key] = strconv.Itoa(magicInfo.Width)
			} else {
				return message, ErrGetCallbackMagicParameter
			}
			break
		case "${image.format}":
			if magicInfo.Format != "" {
				info[key] = magicInfo.Format
			} else {
				return message, ErrGetCallbackMagicParameter
			}
			break
		}
	}
	// Inject user-specified constants
	for k, v := range message.Constant {
		if !strings.HasPrefix(v, "$") {
			info[k] = v
		}
	}
	// Inject user-specified custom variables
	for k, v := range message.Location {
		if !strings.HasPrefix(v, "$") {
			info[k] = v
		}
	}
	message.Infos = info
	return message, nil
}

func PostCallbackMessage(message CallBackMessage) (result string, err error) {
	client := http.Client{
		Timeout: MaxCallbackTimeout,
	}
	req, err := newPostRequest(message)
	if err != nil {
		helper.Logger.Warn("Callback error with getPostRequest:", err)
		return "", ErrCallBackFailed
	}
	helper.Logger.Println("The generated user's callback request is", req, message)
	resp, err := client.Do(req)
	defer resp.Body.Close()
	if err != nil {
		resp.Body.Close()
		if err == http.ErrHandlerTimeout {
			resp, err = doWithRetry(req, 1)
			if err != nil {
				return "", err
			}
		}
	}
	info, err := ioutil.ReadAll(io.LimitReader(resp.Body, MaxBodySize))
	if err != nil {
		helper.Logger.Warn("Callback error with readResponse:", err)
		return "", ErrCallBackFailed
	}
	return string(info), nil
}

func newPostRequest(message CallBackMessage) (*http.Request, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	for k, v := range message.Infos {
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
		signature := getSignatureForCallback(message.Credential, date)
		req.Header.Add(Authorization, signature)
	}
	req.Header.Add(ContentType, writer.FormDataContentType())
	req.Header.Add(Date, date)
	return req, nil
}

func doWithRetry(req *http.Request, times int) (resp *http.Response, err error) {
	client := http.Client{
		Timeout: MaxCallbackTimeout,
	}
	for i := 0; i < times; i++ {
		resp, err = client.Do(req)
		if err != nil {
			if err == http.ErrHandlerTimeout {
				continue
			}
			helper.Logger.Warn("Callback error with doRequest:", err)
			return nil, ErrCallBackFailed
		} else {
			break
		}
	}
	if err != nil {
		if err == http.ErrHandlerTimeout {
			helper.Logger.Warn("Callback error with doRequest Timeout:", err)
			return nil, ErrCallBackFailed
		}
		helper.Logger.Warn("Callback error with doRequest:", err)
		return nil, ErrCallBackFailed
	}
	return resp, nil
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

func GetImageInfoFromReader(reader io.Reader) (height, width int, imageType string, err error) {
	img, imageType, err := image.Decode(reader)
	if err != nil {
		return 0, 0, "", ErrInvalidCallbackMagicImageType
	}
	bounds := img.Bounds()
	width = bounds.Max.X
	height = bounds.Max.Y
	return
}
