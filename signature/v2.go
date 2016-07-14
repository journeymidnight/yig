package signature

import (
	"github.com/kataras/iris"
	"crypto/hmac"
	"strings"
	"sort"
	"time"
	"encoding/base64"
	"net/url"
	"net/http"
	"crypto/sha1"

	"git.letv.cn/yig/yig/minio/datatype"
	"strconv"
	"git.letv.cn/yig/yig/iam"
)



const (
	SignV2Algorithm = "AWS"
	HOST_URL	= "127.0.0.1" /* should be something like
					s3.lecloud.com
					for production servers
					*/
)

func postRequestAuth(c *iris.Context) {

}

func verifyDate(dateString string) (bool, error) {
	date, err := datatype.ParseAmzDate(dateString)
	if err != datatype.ErrNone {
		return false, error("ErrMalformedDate")
	}
	now := time.Now()
	diff := now.Sub(date)
	if diff > 15 * time.Minute || diff < -15 * time.Minute {
		return false, nil
	}
	return true, nil
}

func verifyNotExpires(dateString string) (bool, error) {
	t, err := strconv.ParseInt(dateString, 10, 64)
	if err != nil {
		return false, err
	}
	expires := time.Unix(t, 0)
	now := time.Now()
	if now.After(expires) {
		return false, nil
	}
	return true, nil
}

func buildCanonicalizedAmzHeaders(headers *http.Header)  string {
	var amzHeaders []string
	for k, _ := range *headers {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-") {
			amzHeaders = append(amzHeaders, k)
		}
	}
	sort.Strings(amzHeaders)
	ans := ""
	// TODO use bytes.Buffer
	for _, h := range amzHeaders {
		values := (*headers)[h]  // Don't use Header.Get() here because we need ALL values
		ans += strings.ToLower(h) + ":" + strings.Join(values, ",") + "\n"
	}
	return ans
}

func buildCanonicalizedResource(req *http.Request)  string {
	ans := ""
	if strings.HasSuffix(req.Host, "." + HOST_URL) {
		bucket := strings.TrimSuffix(req.Host, "." + HOST_URL)
		ans += "/" + bucket
	} else if req.Host != "" && req.Host != HOST_URL{
		ans += "/" + req.Host
	}
	ans += req.URL.RawPath
	requiredQuery := []string{
		"acl", "delete", "lifecycle", "location",
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
	queryToSign := url.Values{}
	for _, q := range(requiredQuery) {
		if v, ok := requestQuery[q]; ok {
			queryToSign[q] = v
		}
	}
	if encodedQueryToSign := queryToSign.Encode(); encodedQueryToSign != "" {
		ans += "?" + encodedQueryToSign
	}
	return ans
}

func DoesSignatureMatchV2(r *http.Request) datatype.APIErrorCode {
	authorizationHeader := r.Header.Get("Authorization")
	splitHeader := strings.Split(authorizationHeader, " ")
	// Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature;
	splitSignature := strings.Split(splitHeader[1], ":")
	if len(splitSignature) != 2 {
		return datatype.ErrMissingSignTag
	}
	accessKey := splitSignature[0]
	secretKey, err := iam.GetSecretKey(accessKey)
	if err != nil {
		return datatype.ErrInvalidAccessKeyID
	}
	signature, err := base64.StdEncoding.DecodeString(splitSignature[1])
	if err != nil {
		return datatype.ErrAuthorizationHeaderMalformed
	}
	// StringToSign = HTTP-Verb + "\n" +
	// 	Content-MD5 + "\n" +
	// 	Content-Type + "\n" +
	// 	Date + "\n" +
	// 	CanonicalizedAmzHeaders +
	// 	CanonicalizedResource;
	// Content-MD5 and Content-Type are optional
	stringToSign := r.Method + "\n"
	stringToSign += r.Header.Get("Content-Md5") + "\n"
	stringToSign += r.Header.Get("Content-Type") + "\n"

	date := r.Header.Get("x-amz-date")
	if date == "" {
		date = r.Header.Get("Date")
	}
	if date == "" {
		return datatype.ErrMissingDateHeader
	}
	if verified, err := verifyDate(date); err != nil {
		return datatype.ErrMalformedDate
	} else if !verified {
		return datatype.ErrRequestTimeTooSkewed
	}
	stringToSign += date + "\n"

	stringToSign += buildCanonicalizedAmzHeaders(&r.Header)
	stringToSign += buildCanonicalizedResource(r)

	mac := hmac.New(sha1.New, []byte(secretKey))
	mac.Write([]byte(stringToSign))
	expectedMac := mac.Sum(nil)
	if !hmac.Equal(expectedMac, signature) {
		return datatype.ErrAccessDenied
	}
	return datatype.ErrNone
}

func DoesPresignedSignatureMatch(r *http.Request) datatype.APIErrorCode {
	query := r.URL.Query()
	accessKey := query.Get("AWSAccessKeyId")
	expires := query.Get("Expires")
	signatureString := query.Get("Signature")

	secretKey, err := iam.GetSecretKey(accessKey)
	if err != nil {
		return datatype.ErrInvalidAccessKeyID
	}
	signature, err := base64.StdEncoding.DecodeString(signatureString)
	if err != nil {
		return datatype.ErrAuthorizationHeaderMalformed
	}
	if verified, err := verifyNotExpires(expires); err != nil {
		return datatype.ErrMalformedDate
	} else if !verified {
		return datatype.ErrExpiredPresignRequest

	}
	// StringToSign = HTTP-VERB + "\n" +
	// Content-MD5 + "\n" +
	// Content-Type + "\n" +
	// Expires + "\n" +
	// CanonicalizedAmzHeaders +
	// CanonicalizedResource;
	stringToSign := r.Method + "\n"
	stringToSign += r.Header.Get("Content-Md5") + "\n"
	stringToSign += r.Header.Get("Content-Type") + "\n"
	stringToSign += expires + "\n"
	stringToSign += buildCanonicalizedAmzHeaders(&r.Header)
	stringToSign += buildCanonicalizedResource(r)

	mac := hmac.New(sha1.New, []byte(secretKey))
	mac.Write([]byte(stringToSign))
	expectedMac := mac.Sum(nil)
	if !hmac.Equal(expectedMac, signature) {
		return datatype.ErrAccessDenied
	}
	return datatype.ErrNone
}

