package _go

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/xml"
	"fmt"
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

func Test_CallbackPutObject(t *testing.T) {
	go server()
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
	req.Header.Add("X-Uos-Callback-Body", "bucket=${bucket}&filename=${filename}&etag=${etag}&objectSize=${objectSize}&mimeType=${mimeType}&createTime=${createTime}&height=${image.height}&width=${image.width}&format=${image.format}&location=${x-uos-callback-customize-test}")
	req.Header.Add("X-Uos-Callback-Auth", "1")
	req.Header.Add("X-Uos-Callback-Customize-Test", "test")
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

func Test_CallbackCompleteMultipartUpload(t *testing.T) {
	go server()
	sc := NewS3()
	sc.MakeBucket(TestCallbackBucket)
	defer sc.DeleteBucket(TestCallbackBucket)
	type SConfig struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadId string   `xml:"UploadId"`
	}
	baseurl := "http://" + TestCallbackBucket + "." + Endpoint + "/" + TestCallbackObject
	url := baseurl + "?uploads"
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		t.Error(err)
	}
	client := http.Client{}
	req.Header.Add("Content-Type", "text/pain")
	req.Header.Add("x-amz-storage-class", "STANDARD")
	req.Header.Add("x-amz-date", time.Now().UTC().Format(Iso8601Format))
	signature := GetSignatureV2(req, AccessKey, SecretKey)
	req.Header.Add("Authorization", signature)
	v := SConfig{}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(" createmultipartupload")
	data, err := ioutil.ReadAll(res.Body)
	//	v := SConfig{}
	defer res.Body.Close()
	err = xml.Unmarshal(data, &v)
	if err != nil {
		t.Error(err)
	}
	t.Log("createmultipartupload!", v)
	t.Log("uploadid", v.UploadId)

	// start upload parts
	fd1 := make([]byte, 5242880)
	fdb := bytes.NewReader(fd1)
	uploadid := v.UploadId
	etag := make(map[string]string)
	for i := 1; i < 3; i++ {
		number := strconv.Itoa(i)
		urlupload := baseurl + "?partNumber=" + number + "&uploadId=" + uploadid
		req_upload, _ := http.NewRequest("PUT", urlupload, fdb)
		req_upload.Header.Add("Content-Type", "text/pain")
		req.Header.Add("Content-Length", "5242880")
		req_upload.Header.Add("x-amz-date", time.Now().UTC().Format(Iso8601Format))
		signature_upload := GetSignatureV2(req_upload, AccessKey, SecretKey)
		req_upload.Header.Add("Authorization", signature_upload)
		res_upload, err := client.Do(req_upload)
		etag[number] = res_upload.Header.Get("Etag")
		if err != nil {
			t.Error(err)
		}
		t.Log("uploadpart:", i)
		t.Log("print uploadprocess", etag[number])
	}

	// complete multipartupload
	CompleteMultipartUploadxml := `<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<Part>
                <ETag>` + etag["1"] + `</ETag>
				<PartNumber>1</PartNumber>
			</Part>
			<Part>
                <ETag>` + etag["2"] + `</ETag>
				<PartNumber>2</PartNumber>
			</Part>
			</CompleteMultipartUpload>`
	//	reqbody := bytes.NewReader(CompleteMultipartUpload)
	//	form := CompleteMultipartUpload{}
	//	formbody := xml.Unmarshal(([]byte(CompleteMultipartUploadxml), &form)
	urlComplete := baseurl + "?uploadId=" + uploadid
	reqComplete, err := http.NewRequest("POST", urlComplete, strings.NewReader(CompleteMultipartUploadxml))
	reqComplete.Header.Add("x-amz-date", time.Now().UTC().Format(Iso8601Format))
	reqComplete.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	reqComplete.Header.Add("X-Uos-Callback-Url", "http://127.0.0.1:9099/testcallback")
	reqComplete.Header.Add("X-Uos-Callback-Body", "b=${bucket}&name=${filename}&objectS=${objectSize}&location=${x-uos-callback-customize-test}&image=${x-uos-callback-customize-image}")
	reqComplete.Header.Add("X-Uos-Callback-Auth", "1")
	reqComplete.Header.Add("x-uos-callback-customize-test", "test")
	reqComplete.Header.Add("x-uos-callback-customize-image", "1")
	signatureComplete := GetSignatureV2(reqComplete, AccessKey, SecretKey)
	reqComplete.Header.Add("Authorization", signatureComplete)
	respComplete, err := client.Do(reqComplete)
	if err != nil {
		t.Error("completemultipartupload falied", err)
	}
	defer sc.DeleteObject(TestCallbackBucket, TestCallbackObject)

	if respComplete.Header.Get("X-Uos-Callback-Result") != "It is OK!" {
		t.Error("The callback return parameter is not obtained", respComplete)
	}

}

func Test_CallbackPostObject(t *testing.T) {
	go server()
	sc := NewS3()
	sc.MakeBucket(TestCallbackBucket)
	defer sc.DeleteBucket(TestCallbackBucket)
	pbi := &PostObjectInput{
		Url:        fmt.Sprintf("http://"+Endpoint+"/%s", TestCallbackBucket),
		Bucket:     TestCallbackBucket,
		ObjName:    TestCallbackObject,
		Expiration: time.Now().UTC().Add(time.Duration(1 * time.Hour)),
		Date:       time.Now().UTC(),
		Region:     "r",
		AK:         AccessKey,
		SK:         SecretKey,
		FileSize:   1024,
	}
	url := "http://127.0.0.1:9099/testcallback"
	body := "b=${bucket}&name=${filename}&objectS=${objectSize}&location=${X-Uos-Callback-Customize-Test}&image=${X-Uos-Callback-Customize-Image}"
	body = base64.StdEncoding.EncodeToString([]byte(body))
	auth := "1"
	info := make(map[string]string)
	info["X-Uos-Callback-Customize-Test"] = "test"
	info["X-Uos-Callback-Customize-Image"] = "1"
	resp, err := sc.PostObjectWithCallback(pbi, url, body, auth, info)
	defer resp.Body.Close()
	if err != nil {
		t.Fatal("PostObject err:", err)
	}
	t.Log("PostObject Success!", resp.Header)
	if resp.Header.Get("X-Uos-Callback-Result") != "It is OK!" {
		t.Error("The callback return parameter is not obtained", resp)
	}
	defer sc.DeleteObject(TestCallbackBucket, TestCallbackObject)
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

func server() {
	http.HandleFunc("/testcallback", handler)
	http.ListenAndServe(":9099", nil)
}

func handler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(1024 << 10)
	if err != nil {
		fmt.Println("ParseMultipartForm err:", err)
	}

	date := r.Header.Get(Date)
	signature := getSignatureForCallback(date)
	auth := r.Header.Get(Authorization)
	if auth != signature {
		fmt.Println("Signature verification failed")
	}

	if r.MultipartForm != nil {
		if v, ok := r.MultipartForm.Value["image"]; ok && v[0] != "1" {
			if _, ok := r.MultipartForm.Value["width"]; ok {
				fmt.Println("Failed to get picture parameters")
			}
		}
		fmt.Println(r.MultipartForm.Value)
	}
	_, err = w.Write([]byte("It is OK!"))
	if err != nil {
		fmt.Println("Write return info err:", err)
	}
	w.WriteHeader(http.StatusOK)
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
