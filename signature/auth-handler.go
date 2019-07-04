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

package signature

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io/ioutil"
	"net/http"
	"strings"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/iam/common"
)

// Verify if request has AWS Signature
// for v2, the Authorization header starts with "AWS ",
// for v4, starts with "AWS4-HMAC-SHA256 " (notice the space after string)
func isRequestSignature(r *http.Request) (bool, AuthType) {
	if _, ok := r.Header["Authorization"]; ok {
		if len(r.Header.Get("Authorization")) == 0 {
			return false, AuthTypeUnknown
		}
		header := r.Header.Get("Authorization")
		if strings.HasPrefix(header, signV4Algorithm+" ") {
			return true, AuthTypeSignedV4
		} else if strings.HasPrefix(header, SignV2Algorithm+" ") {
			return true, AuthTypeSignedV2
		}
	}
	return false, AuthTypeUnknown
}

// Verify if request is AWS presigned
func isRequestPresigned(r *http.Request) (bool, AuthType) {
	if _, ok := r.URL.Query()["X-Amz-Credential"]; ok {
		return true, AuthTypePresignedV4
	} else if _, ok := r.URL.Query()["AWSAccessKeyId"]; ok {
		return true, AuthTypePresignedV2
	}
	return false, AuthTypeUnknown
}

// Verify if request is of type AWS POST policy Signature
func isRequestPostPolicySignature(r *http.Request) bool {
	if r.Method != "POST" {
		return false
	}
	if _, ok := r.Header["Content-Type"]; ok {
		if strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") {
			return true
		}
	}
	return false
}

// Verify if the request has AWS Streaming Signature Version '4'. This is only valid for 'PUT' operation.
func isRequestSignStreamingV4(r *http.Request) bool {
	return r.Header.Get("X-Amz-Content-Sha256") == streamingContentSHA256 &&
		r.Method == http.MethodPut
}

// Authorization type.
type AuthType int

// List of all supported auth types.
const (
	AuthTypeUnknown AuthType = iota
	AuthTypeAnonymous
	AuthTypePresignedV4
	AuthTypePresignedV2
	AuthTypePostPolicy
	AuthTypeStreamingSigned
	AuthTypeSignedV4
	AuthTypeSignedV2
)

// Get request authentication type.
func GetRequestAuthType(r *http.Request) AuthType {
	if isRequestSignStreamingV4(r) {
		return AuthTypeStreamingSigned
	} else if isSignature, version := isRequestSignature(r); isSignature {
		return version
	} else if isPresigned, version := isRequestPresigned(r); isPresigned {
		return version
	} else if isRequestPostPolicySignature(r) {
		return AuthTypePostPolicy
	} else if _, ok := r.Header["Authorization"]; !ok {
		return AuthTypeAnonymous
	}
	return AuthTypeUnknown
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
func IsReqAuthenticated(r *http.Request) (c common.Credential, e error) {
	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return c, ErrInternalError
	}
	// Verify Content-Md5, if payload is set.
	if r.Header.Get("Content-Md5") != "" {
		if r.Header.Get("Content-Md5") != base64.StdEncoding.EncodeToString(sumMD5(payload)) {
			return c, ErrBadDigest
		}
	}
	// Populate back the payload.
	r.Body = ioutil.NopCloser(bytes.NewReader(payload))
	validateRegion := false // TODO: Validate region.
	switch GetRequestAuthType(r) {
	case AuthTypePresignedV4:
		return DoesPresignedSignatureMatchV4(r, validateRegion)
	case AuthTypeSignedV4:
		return DoesSignatureMatchV4(hex.EncodeToString(sum256(payload)), r, validateRegion)
	case AuthTypePresignedV2:
		return DoesPresignedSignatureMatchV2(r)
	case AuthTypeSignedV2:
		return DoesSignatureMatchV2(r)
	case AuthTypeStreamingSigned:
		credential, _, _, _, err := CalculateSeedSignature(r)
		return credential, err
	}
	return c, ErrAccessDenied
}
