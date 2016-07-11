/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package minio

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io/ioutil"
	"net/http"
	"strings"

	"git.letv.cn/yig/yig/signature"
	. "git.letv.cn/yig/yig/minio/datatype"
)

// Verify if request has AWS Signature
// for v2, the Authorization header starts with "AWS ",
// for v4, starts with "AWS4-HMAC-SHA256 " (notice the space after string)
func isRequestSignature(r *http.Request) (bool, authType) {
	if _, ok := r.Header["Authorization"]; ok {
		header := r.Header.Get("Authorization")
		if strings.HasPrefix(header, signV4Algorithm + " ") {
			return true, authTypeSigned
		} else if strings.HasPrefix(header, signature.SignV2Algorithm + " ") {
			return true, authTypeSignedV2
		}
	}
	return false, nil
}

// Verify if request is AWS presigned
func isRequestPresigned(r *http.Request) (bool, authType) {
	if _, ok := r.URL.Query()["X-Amz-Credential"]; ok {
		return true, authTypePresigned
	} else if _, ok := r.URL.Query()["AWSAccessKeyId"]; ok {
		return true, authTypePresignedV2
	}
	return false, nil
}

// Verify if request is of type AWS POST policy Signature
func isRequestPostPolicySignature(r *http.Request) bool {
	if r.Method != "POST" {
		return false
	}
	if _, ok := r.Header["Content-Type"]; ok {
		if strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") && r.Method == "POST" {
			return true
		}
	}
	return false
}

// Authorization type.
type authType int

// List of all supported auth types.
const (
	authTypeUnknown authType = iota
	authTypeAnonymous
	authTypePresigned  // v4
	authTypePresignedV2
	authTypePostPolicy // including v2 and v4, handled specially in API endpoint
	authTypeSigned  // v4
	authTypeSignedV2
)

// Get request authentication type.
func getRequestAuthType(r *http.Request) authType {
	if isSignature, version := isRequestSignature(r); isSignature {
		return version
	} else if isPresigned, version := isRequestPresigned(r); isPresigned {
		return version
	} else if isRequestPostPolicySignature(r) {
		return authTypePostPolicy
	} else if _, ok := r.Header["Authorization"]; !ok {
		return authTypeAnonymous
	}
	return authTypeUnknown
}

// sum256 calculate sha256 sum for an input byte array
func sum256(data []byte) []byte {
	hash := sha256.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// sumMD5 calculate md5 sum for an input byte array
func sumMD5(data []byte) []byte {
	hash := md5.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// A helper function to verify if request has valid AWS Signature
func isReqAuthenticated(r *http.Request) (s3Error APIErrorCode) {
	if r == nil {
		return ErrInternalError
	}
	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return ErrInternalError
	}
	// Verify Content-Md5, if payload is set.
	if r.Header.Get("Content-Md5") != "" {
		if r.Header.Get("Content-Md5") != base64.StdEncoding.EncodeToString(sumMD5(payload)) {
			return ErrBadDigest
		}
	}
	// Populate back the payload.
	r.Body = ioutil.NopCloser(bytes.NewReader(payload))
	validateRegion := true // TODO: Validate region.
	switch getRequestAuthType(r) {
	case authTypePresigned:
		return doesPresignedSignatureMatch(hex.EncodeToString(sum256(payload)), r, validateRegion)
	case authTypeSigned:
		return doesSignatureMatch(hex.EncodeToString(sum256(payload)), r, validateRegion)
	case authTypePresignedV2:
		return signature.DoesPresignedSignatureMatch(r)
	case authTypeSignedV2:
		return signature.DoesSignatureMatchV2(r)
	}
	return ErrAccessDenied
}

// authHandler - handles all the incoming authorization headers and
// validates them if possible.
type authHandler struct {
	handler http.Handler
}

// setAuthHandler to validate authorization header for the incoming request.
func setAuthHandler(h http.Handler) http.Handler {
	return authHandler{h}
}

// handler for validating incoming authorization headers.
func (a authHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch getRequestAuthType(r) {
	case authTypeUnknown:
		writeErrorResponse(w, r, ErrSignatureVersionNotSupported, r.URL.Path)
		return
	default:
		// Let top level caller validate for anonymous and known
		// signed requests.
		a.handler.ServeHTTP(w, r)
		return
	}
}
