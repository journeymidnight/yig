package _go

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"github.com/gin-gonic/gin"
	. "github.com/journeymidnight/yig/test/go/lib"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	Iso8601Format         = "20060102T150405Z"
	TestCallbackObject    = "test.png"
	Authorization         = "Authorization"
	Date                  = "X-Uos-Date"
	CallbackAuthorization = "UOS-CALLBACK-AUTH"
	CallbackResult        = "X-Uos-Callback-Result"
)

var domain = []string{"s3.test.com"}

func Test_Callback(t *testing.T) {
	go server(t)
	sc := NewS3()
	sc.MakeBucket(TestCallbackBucket)
	defer sc.DeleteBucket(TestCallbackBucket)
	url := "http://" + TestCallbackBucket + "." + Endpoint + "/" + TestCallbackObject
	resp, err := http.Get("https://www.baidu.com/img/bd_logo1.png")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	payload, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Error(err)
	}
	size := uint64(len(payload))
	reader := bytes.NewReader(payload)
	req, _ := http.NewRequest("PUT", url, reader)
	req.Header.Add("Content-Type", "image/png")
	req.Header.Add("Content-Length", strconv.FormatUint(size, 10))
	req.Header.Add("x-amz-storage-class", "STANDARD")
	req.Header.Add("x-amz-date", time.Now().UTC().Format(Iso8601Format))
	req.Header.Add("X-Uos-Callback-Url", "http://127.0.0.1:9099/testcallback")
	req.Header.Add("X-Uos-Callback-Body", "bucket=${bucket}&filename=${filename}&etag=${etag}&objectSize=${objectSize}&mimeType=${mimeType}&createTime=${createTime}&height=${height}&width=${width}&location=${x-uos-callback-customize-test}")
	req.Header.Add("X-Uos-Callback-Auth", "1")
	req.Header.Add("x-uos-callback-custom-test", "test")
	signature := GetSignatureV2(req, AccessKey, SecretKey)
	req.Header.Add("Authorization", signature)
	res, err := http.DefaultClient.Do(req)
	defer sc.DeleteObject(TestCallbackBucket, TestCallbackObject)
	if err != nil {
		t.Error(err)
	}
	defer res.Body.Close()
	if res.Status != "200 OK" {
		t.Error(res.Status, err)
	}
	if res.Header.Get("X-Uos-Callback-Result") != "It is OK!" {
		t.Error("The callback return parameter is not obtained")
	}
}

func GetSignatureV2(r *http.Request, ak, sk string) (signature string) {
	stringToSign := r.Method + "\n"
	stringToSign += r.Header.Get("Content-Md5") + "\n"
	stringToSign += r.Header.Get("Content-Type") + "\n"
	amzDateHeaderIncluded := true
	date := r.Header.Get("x-amz-date")
	if date == "" {
		amzDateHeaderIncluded = false
		date = r.Header.Get("Date")
	}
	if amzDateHeaderIncluded {
		stringToSign += "\n"
	} else {
		stringToSign += date + "\n"
	}
	stringToSign += buildCanonicalizedAmzHeaders(&r.Header)
	stringToSign += buildCanonicalizedResource(r)
	mac := hmac.New(sha1.New, []byte(sk))
	mac.Write([]byte(stringToSign))
	expectedMac := mac.Sum(nil)
	return "AWS " + ak + ":" + base64.StdEncoding.EncodeToString(expectedMac)
}

func buildCanonicalizedAmzHeaders(headers *http.Header) string {
	var amzHeaders []string
	for k := range *headers {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-") {
			amzHeaders = append(amzHeaders, k)
		}
	}
	sort.Strings(amzHeaders)
	ans := ""
	for _, h := range amzHeaders {
		values := (*headers)[h] // Don't use Header.Get() here because we need ALL values
		ans += strings.ToLower(h) + ":" + strings.Join(values, ",") + "\n"
	}
	return ans
}

func buildCanonicalizedResource(req *http.Request) string {
	ans := ""
	v := strings.Split(req.Host, ":")
	hostWithOutPort := v[0]
	ok, bucketName := hasBucketInDomain(hostWithOutPort, domain)
	if ok {
		ans += "/" + bucketName
	}
	ans += req.URL.EscapedPath()
	requiredQuery := []string{
		// NOTE: this array is sorted alphabetically
		"acl", "cors", "delete", "lifecycle", "location",
		"logging", "notification", "partNumber",
		"policy", "requestPayment",
		"response-cache-control",
		"response-content-disposition",
		"response-content-encoding",
		"response-content-language",
		"response-content-type",
		"response-expires",
		"torrent", "uploadId", "uploads", "versionId",
		"versioning", "versions", "website",
	}
	requestQuery := req.URL.Query()
	encodedQuery := ""
	for _, q := range requiredQuery {
		if values, ok := requestQuery[q]; ok {
			for _, v := range values {
				if encodedQuery != "" {
					encodedQuery += "&"
				}
				if v == "" {
					encodedQuery += q
				} else {
					encodedQuery += q + "=" + v
				}
			}
		}
	}
	if encodedQuery != "" {
		ans += "?" + encodedQuery
	}
	return ans
}

func hasBucketInDomain(host string, domains []string) (ok bool, bucket string) {
	for _, d := range domains {
		suffix := "." + d
		if strings.HasSuffix(host, suffix) {
			return true, strings.TrimSuffix(host, suffix)
		}
	}
	return false, ""
}

func server(t *testing.T) {
	// Engin
	router := gin.Default()
	//router := gin.New()

	router.POST("/testcallback", func(c *gin.Context) {
		c.Request.ParseMultipartForm(1024 << 10)

		date := c.Request.Header.Get(Date)
		signature := getSignatureForCallback(date)
		auth := c.Request.Header.Get(Authorization)
		if auth != signature {
			t.Error("Signature verification failed")
		}

		if c.Request.MultipartForm != nil {
			if c.Request.MultipartForm.Value["width"][0] == "" {
				t.Error("Failed to get picture parameters")
			}
			t.Log(c.Request.MultipartForm.Value)
		}
		c.String(http.StatusOK, "It is OK!")
	})

	router.GET("/test", func(context *gin.Context) {
		context.String(http.StatusOK, "Hello!")
	})

	router.Run("0.0.0.0:9099")
}

func getSignatureForCallback(date string) string {
	var key, data string
	key = SecretKey
	data = "POST\n" + date
	mac := hmac.New(sha1.New, []byte(key))
	mac.Write([]byte(data))
	signature := mac.Sum(nil)
	return CallbackAuthorization + " " + AccessKey + ":" + base64.StdEncoding.EncodeToString(signature)
}
