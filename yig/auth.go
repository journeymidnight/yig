package main

import (
	"net/http"
	"github.com/codegangsta/martini"
	"strings"
	"io/ioutil"
	"encoding/hex"
	"crypto/md5"
	"time"
	"encoding/base64"
	"sort"
	"net/url"
	"crypto/hmac"
	"crypto/sha1"
	"text/scanner"
)

func postRequestAuth(req *http.Request, res http.ResponseWriter, context martini.Context) {

}

func ensureRequestBody(req *http.Request, requestContext *RequestContext) error {
	if requestContext.requestBody == nil {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			return err
		}
		requestContext.requestBody = &body
	}
}

func verifyMd5(requestBody *[]byte, contentMd5 string) bool {
	hasher := md5.New()
	hasher.Write(*requestBody)
	expectedMd5 := hex.EncodeToString(hasher.Sum(nil))
	decodedMd5, err := base64.StdEncoding.DecodeString(contentMd5)
	if err != nil {
		return false
	}
	return decodedMd5 == expectedMd5
}

func verifyDate(dateString string) bool {
	date, err := time.Parse("Mon, 02 Jan 2006 15:04:05 -0700", dateString)
	if err != nil {
		return false
	}
	now := time.Now()
	diff := now.Sub(date)
	if diff > 15 * time.Minute || diff < -15 * time.Minute {
		return false
	}
	return true
}

func buildCanonicalizedAmzHeaders(headers *map[string][]string)  string {
	var amzHeaders []string
	for k, _ := range(headers) {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-") {
			amzHeaders = append(amzHeaders, k)
		}
	}
	sort.Strings(amzHeaders)
	ans := ""
	for _, h := range(amzHeaders) {
		values := headers[h]
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

func getSecretKey(accessKey string) (secretKey string, err error) {
	// should use a cache with timeout
}

func authorizationHeaderAuth(req *http.Request, res http.ResponseWriter, requestContext *RequestContext) {
	authorizationHeader := req.Header.Get("Authorization")
	splitHeader := strings.Split(authorizationHeader, " ")
	version := splitHeader[0]
	if version == "AWS" { // v2
		// Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature;
		splitSignature := strings.Split(splitHeader[1], ":")
		if len(splitSignature) != 2 {
			responseWithError(res, &ErrorResponse{
				StatusCode:http.StatusBadRequest,
				Code: "InvalidRequest",
				Message: "Malformed authorization header",
			})
			return
		}
		accessKey := splitSignature[0]
		secretKey, err := getSecretKey(accessKey)
		if err != nil {
			responseWithError(res, &ErrorResponse{
				StatusCode:http.StatusForbidden,
				Code:"AccessDenied",
				Message:"Your access key does not exist",
			})
			return
		}
		signature, err := base64.StdEncoding.DecodeString(splitSignature[1])
		if err != nil {
			responseWithError(res, &ErrorResponse{
				StatusCode:http.StatusBadRequest,
				Code:"InvalidRequest",
				Message:"Authorization signature cannot be decoded",
			})
			return
		}
		/*
		StringToSign = HTTP-Verb + "\n" +
			Content-MD5 + "\n" +
			Content-Type + "\n" +
			Date + "\n" +
			CanonicalizedAmzHeaders +
			CanonicalizedResource;
		Content-MD5 and Content-Type are optional
		 */
		stringToSign := req.Method + "\n"

		if md5 := req.Header.Get("Content-MD5"); md5 != "" {
			err := ensureRequestBody(req, requestContext)
			if err != nil {
				responseWithError(res, &ErrorResponse{
					StatusCode:http.StatusBadRequest,
					Code:"InvalidRequest",
					Message:"Bad request body",
				})
				return
			}
			if !verifyMd5(requestContext.requestBody, md5) {
				responseWithError(res, &ErrorResponse{
					StatusCode:http.StatusBadRequest,
					Code:"BadDigest",
					Message:"The Content-MD5 you specified did not match what we received",
				})
				return
			}
			stringToSign += md5 + "\n"
		} else {
			stringToSign += "\n"
		}

		stringToSign += req.Header.Get("Content-Type") + "\n"

		date := req.Header.Get("x-amz-date")
		if date == "" {
			date = req.Header.Get("Date")
		}
		if date == "" {
			responseWithError(res, &ErrorResponse{
				StatusCode:http.StatusBadRequest,
				Code:"InvalidRequest",
				Message: "No Date and x-amz-date header provided",
			})
			return
		}
		if !verifyDate(date) {
			responseWithError(res, &ErrorResponse{
				StatusCode:http.StatusForbidden,
				Code:"RequestTimeTooSkewed",
				Message:"The difference between the request time and the server's time is too large",
			})
			return
		}
		stringToSign += date + "\n"

		stringToSign += buildCanonicalizedAmzHeaders(&req.Header)

		stringToSign += buildCanonicalizedResource(req)

		mac := hmac.New(sha1.New, []byte(secretKey))
		mac.Write([]byte(stringToSign))
		expectedMac := mac.Sum(nil)
		if !hmac.Equal(expectedMac, signature) {
			responseWithError(res, &ErrorResponse{
				StatusCode:http.StatusForbidden,
				Code:"AccessDenied",
				Message:"Access Denied",
			})
		}
	} else if version == "AWS4-HMAC-SHA256" { // v4

	} else {
		responseWithError(res, &ErrorResponse{
			StatusCode:http.StatusBadRequest,
			Code:"UnexpectedContent",
			Message: "Unsupported authorization method: " + version,
		})
		return
	}
}

func queryParameterAuth(req *http.Request, res http.ResponseWriter, context martini.Context) {

}

func awsAuth(req *http.Request, res http.ResponseWriter, context martini.Context,
requestContext *RequestContext)  {
	if req.Method == "POST" {
		postRequestAuth(req, res, context)
	} else if req.Header.Get("Authorization") != "" {
		authorizationHeaderAuth(req, res, requestContext)
	} else {
		queryParameterAuth(req, res, context)
	}
}
