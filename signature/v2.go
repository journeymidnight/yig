package signature

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"net/http"
	"sort"
	"strings"
	"time"

	"errors"
	"git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/iam"
	"strconv"
	"net"
)

const (
	SignV2Algorithm = "AWS"
	SignV4Algorithm = "AWS4-HMAC-SHA256"
	/*HOST_URL        = "s3.test.com:3000"  should be something like
	s3.lecloud.com
	for production servers
	*/
)

func verifyDate(dateString string) (bool, error) {
	date, err := datatype.ParseAmzDate(dateString)
	if err != nil {
		return false, errors.New("ErrMalformedDate")
	}
	now := time.Now()
	diff := now.Sub(date)
	if diff > 15*time.Minute || diff < -15*time.Minute {
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

func buildCanonicalizedAmzHeaders(headers *http.Header) string {
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
		values := (*headers)[h] // Don't use Header.Get() here because we need ALL values
		ans += strings.ToLower(h) + ":" + strings.Join(values, ",") + "\n"
	}
	helper.Debugln("V2 canonical amazon headers:", ans)
	return ans
}

func buildCanonicalizedResource(req *http.Request) string {
	ans := ""
	_, port, _:= net.SplitHostPort(helper.Cfg.BindApiAddress)
	HOST_URL := helper.Cfg.S3Domain + ":" +port
	if strings.HasSuffix(req.Host, "."+HOST_URL) {
		bucket := strings.TrimSuffix(req.Host, "."+HOST_URL)
		ans += "/" + bucket
	} else if req.Host != "" && req.Host != HOST_URL {
		ans += "/" + req.Host
	}
	ans += req.URL.EscapedPath()
	helper.Logger.Println("HOST:",req.Host, HOST_URL,ans)
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
	helper.Debugln("V2 canonical resource:", ans)
	return ans
}

// Calculate HMAC and compare with signature from client
func dictate(secretKey string, stringToSign string, signature []byte) error {
	mac := hmac.New(sha1.New, []byte(secretKey))
	mac.Write([]byte(stringToSign))
	expectedMac := mac.Sum(nil)
	helper.Logger.Println("key，mac",secretKey, string(expectedMac), string(signature))
	if !hmac.Equal(expectedMac, signature) {
		return ErrAccessDenied
	}
	return nil
}

func DoesSignatureMatchV2(r *http.Request) (credential iam.Credential, err error) {
	authorizationHeader := r.Header.Get("Authorization")
	splitHeader := strings.Split(authorizationHeader, " ")
	// Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature;
	splitSignature := strings.Split(splitHeader[1], ":")
	if len(splitSignature) != 2 {
		return credential, ErrMissingSignTag
	}
	accessKey := splitSignature[0]
	credential, e := iam.GetCredential(accessKey)
	if e != nil {
		return credential, ErrInvalidAccessKeyID
	}
	signature, e := base64.StdEncoding.DecodeString(splitSignature[1])
	if e != nil {
		return credential, ErrAuthorizationHeaderMalformed
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

	amzDateHeaderIncluded := true
	date := r.Header.Get("x-amz-date")
	if date == "" {
		amzDateHeaderIncluded = false
		date = r.Header.Get("Date")
	}
	if date == "" {
		return credential, ErrMissingDateHeader
	}
	if verified, e := verifyDate(date); e != nil {
		return credential, ErrMalformedDate
	} else if !verified {
		return credential, ErrRequestTimeTooSkewed
	}
	// "if you include the x-amz-date header, use the empty string for the Date when
	// constructing the StringToSign."
	// See http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html
	if amzDateHeaderIncluded {
		stringToSign += "\n"
	} else {
		stringToSign += date + "\n"
	}

	stringToSign += buildCanonicalizedAmzHeaders(&r.Header)
	stringToSign += buildCanonicalizedResource(r)
	helper.Logger.Println("stringtosign", stringToSign,credential.SecretAccessKey)
	return credential, dictate(credential.SecretAccessKey, stringToSign, signature)
}

func DoesPresignedSignatureMatchV2(r *http.Request) (credential iam.Credential, err error) {
	query := r.URL.Query()
	accessKey := query.Get("AWSAccessKeyId")
	expires := query.Get("Expires")
	signatureString := query.Get("Signature")

	credential, e := iam.GetCredential(accessKey)
	if e != nil {
		return credential, ErrInvalidAccessKeyID
	}
	signature, e := base64.StdEncoding.DecodeString(signatureString)
	if e != nil {
		return credential, ErrAuthorizationHeaderMalformed
	}
	if verified, e := verifyNotExpires(expires); e != nil {
		return credential, ErrMalformedDate
	} else if !verified {
		return credential, ErrExpiredPresignRequest

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

	return credential, dictate(credential.SecretAccessKey, stringToSign, signature)
}

func DoesPolicySignatureMatchV2(formValues map[string]string) (credential iam.Credential,
	e error) {
	var secretKey string
	var err error
	if accessKey, ok := formValues["Awsaccesskeyid"]; ok {
		credential, err = iam.GetCredential(accessKey)
		if err != nil {
			return credential, ErrInvalidAccessKeyID
		}
	} else {
		return credential, ErrMissingFields
	}

	var signatureString string
	var ok bool
	if signatureString, ok = formValues["Signature"]; !ok {
		return credential, ErrMissingFields
	}
	signature, err := base64.StdEncoding.DecodeString(signatureString)
	if err != nil {
		return credential, ErrMalformedPOSTRequest
	}
	var policy string
	if policy, ok = formValues["Policy"]; !ok {
		return credential, ErrMissingFields
	}

	return credential, dictate(secretKey, policy, signature)
}
