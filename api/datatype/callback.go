package datatype

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
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

const MaxCallbackTimeout = time.Second * 3

type CallBackInfo struct {
	BucketName string
	FileName   string
	Etag       string
	ObjectSize int64
	MimeType   string
	CreateTime uint64
	Height     int
	Width      int
	Location   map[string]string
}

type CallBackMessage struct {
	Url        string
	Auth       bool
	Body       string
	Info       CallBackInfo
	Credential common.Credential
}

func GetCallbackFromHeader(header http.Header) (isCallback bool, message CallBackMessage) {
	message = CallBackMessage{}
	url := header.Get("X-Uos-Callback-Url")
	body := header.Get("X-Uos-Callback-Body")
	if url == "" || body == "" {
		return false, message
	}
	message.Url = url
	auth := header.Get("X-Uos-Callback-Auth")
	if auth == "1" {
		message.Auth = true
	} else {
		message.Auth = false
	}
	message.Body = header.Get("x-uos-callback-body")
	return true, message
}

func GetCallbackFromForm(formValues map[string]string) (isCallback bool, message CallBackMessage) {
	message = CallBackMessage{}
	url := formValues["X-Uos-Callback-Url"]
	body := formValues["X-Uos-Callback-Body"]
	if url == "" || body == "" {
		return false, message
	}
	message.Url = url
	auth := formValues["X-Uos-Callback-Auth"]
	if auth == "1" {
		message.Auth = true
	} else {
		message.Auth = false
	}
	message.Body = formValues["x-uos-callback-body"]
	return true, message
}

func IsCallbackImageInfo(contentType string, objectName string) bool {
	switch contentType {
	case "image/jpeg":
		return true
	case "application/x-jpg":
		return true
	case "image/png":
		return true
	case "application/x-png":
		return true
	case "image/gif":
		return true
	default:
		if strings.HasSuffix(objectName, ".jpg") ||
			strings.HasSuffix(objectName, ".png") ||
			strings.HasSuffix(objectName, ".gif") {
			return true
		}
	}
	return false
}

func ValidCallbackImgInfo(infos string) bool {
	infoMap := make(map[string]string)
	infoSplits := strings.Split(infos, "&")
	for _, split := range infoSplits {
		header := strings.Split(split, "=")
		infoMap[header[0]] = header[1]
	}
	if strings.HasPrefix(infoMap["height"], "${") || strings.HasPrefix(infoMap["width"], "${") {
		return true
	}
	return false
}

func ValidCallbackInfo(infos string, noValidInfo CallBackInfo) CallBackInfo {
	infoMap := make(map[string]string)
	infoSplits := strings.Split(infos, "&")
	for _, split := range infoSplits {
		header := strings.Split(split, "=")
		infoMap[header[0]] = header[1]
	}
	info := CallBackInfo{}
	var err error
	for key, value := range infoMap {
		switch key {
		case "bucket":
			if strings.HasPrefix(value, "${") {
				if noValidInfo.BucketName != "" {
					info.BucketName = noValidInfo.BucketName
				} else {
					info.BucketName = ""
				}
			}
			info.BucketName = noValidInfo.BucketName
		case "filename":
			if strings.HasPrefix(value, "${") {
				if noValidInfo.FileName != "" {
					info.FileName = noValidInfo.FileName
				} else {
					info.FileName = ""
				}
			}
			info.FileName = value
		case "etag":
			if strings.HasPrefix(value, "${") {
				if noValidInfo.Etag != "" {
					info.Etag = noValidInfo.Etag
				} else {
					info.Etag = ""
				}
			}
			info.Etag = value
		case "objectSize":
			if strings.HasPrefix(value, "${") {
				if noValidInfo.ObjectSize != 0 {
					info.ObjectSize = noValidInfo.ObjectSize
				} else {
					info.ObjectSize = 0
				}
			}
			info.ObjectSize, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				info.ObjectSize = 0
			}
		case "mimeType":
			if strings.HasPrefix(value, "${") {
				if noValidInfo.MimeType != "" {
					info.MimeType = noValidInfo.MimeType
				} else {
					info.MimeType = ""
				}
			}
			info.MimeType = value
		case "createTime":
			if strings.HasPrefix(value, "${") {
				if noValidInfo.CreateTime != 0 {
					info.CreateTime = noValidInfo.CreateTime
				} else {
					info.CreateTime = 0
				}
			}
			info.CreateTime, err = strconv.ParseUint(value, 10, 64)
			if err != nil {
				info.CreateTime = 0
			}
		case "height":
			if strings.HasPrefix(value, "${") {
				if noValidInfo.Height != 0 {
					info.Height = noValidInfo.Height
				} else {
					info.Height = 0
				}
			}
			info.Height, err = strconv.Atoi(value)
			if err != nil {
				info.Height = 0
			}
		case "width":
			if strings.HasPrefix(value, "${") {
				if noValidInfo.Width != 0 {
					info.Width = noValidInfo.Width
				} else {
					info.Width = 0
				}
			}
			info.Width, err = strconv.Atoi(value)
			if err != nil {
				info.Width = 0
			}
		default:
			infoMap[key] = value
		}
	}
	return info
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
	if err != nil {
		if err == http.ErrHandlerTimeout {
			resp, err = client.Do(req)
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
	defer resp.Body.Close()
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
	if message.Info.BucketName != "" {
		if err := writer.WriteField("bucket", message.Info.BucketName); err != nil {
			return nil, err
		}
	}
	if message.Info.FileName != "" {
		if err := writer.WriteField("filename", message.Info.FileName); err != nil {
			return nil, err
		}
	}
	if message.Info.Etag != "" {
		if err := writer.WriteField("etag", message.Info.Etag); err != nil {
			return nil, err
		}
	}
	if message.Info.ObjectSize != 0 {
		if err := writer.WriteField("objectSize", strconv.FormatInt(message.Info.ObjectSize, 10)); err != nil {
			return nil, err
		}
	}
	if message.Info.MimeType != "" {
		if err := writer.WriteField("mimeType", message.Info.MimeType); err != nil {
			return nil, err
		}
	}
	if message.Info.CreateTime > 0 {
		if err := writer.WriteField("createTime", strconv.FormatUint(message.Info.CreateTime, 10)); err != nil {
			return nil, err
		}
	}
	if message.Info.Height != 0 && message.Info.Width != 0 {
		if err := writer.WriteField("imageInfo.height", strconv.Itoa(message.Info.Height)); err != nil {
			return nil, err
		}
		if err := writer.WriteField("imageInfo.width", strconv.Itoa(message.Info.Width)); err != nil {
			return nil, err
		}
	}
	for k, v := range message.Info.Location {
		if err := writer.WriteField(k, v); err != nil {
			return nil, err
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
	date := time.Now().UTC()
	if message.Auth {
		signature := getSignatureForCallback(credential, date)
		req.Header.Add("Authorization", signature)
	}
	req.Header.Add("Content-Type", writer.FormDataContentType())
	req.Header.Add("X-Uos-Date", date.String())
	return req, nil
}

func getSignatureForCallback(credential common.Credential, date time.Time) string {
	var key, data string
	key = credential.SecretAccessKey
	data = "POST\n" + date.String()
	mac := hmac.New(sha1.New, []byte(key))
	mac.Write([]byte(data))
	signature := hex.EncodeToString(mac.Sum(nil))
	return "UOS-CALLBACK-AUTH " + credential.AccessKeyID + ":" + base64.StdEncoding.EncodeToString([]byte(signature))
}

type CallbackReader struct {
	isCallback    bool
	isImage       bool
	objectSize    int64
	width, height int
	reader        io.Reader
}

func (r *CallbackReader) Read(b []byte) (n int, err error) {
	if r.isCallback {
		r.objectSize = int64(len(b))
		if r.isImage {
			imageReader := bytes.NewReader(b)
			img, _, err := image.Decode(imageReader)
			if err != nil {
				return 0, err
			}
			bounds := img.Bounds()
			r.width = bounds.Max.X
			r.height = bounds.Max.Y
		}
	}
	return r.reader.Read(b)
}

func (r *CallbackReader) Close() error {
	return nil
}

func (r *CallbackReader) GetCallBackReaderInfos() (objectSize int64, width, height int) {
	return r.objectSize, r.width, r.height
}

func NewCallbackReader(r io.ReadCloser, isCallback bool, isImage bool) CallbackReader {
	return CallbackReader{
		isCallback: isCallback,
		reader:     r,
		isImage:    isImage,
	}
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
