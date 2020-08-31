// Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crypto

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"net/http"
	"strings"

	. "github.com/journeymidnight/yig/brand"
)

const (
	// SSEAlgorithmAES256 is the only supported value for the SSE-S3 or SSE-C algorithm header.
	// For SSE-S3 see: https://docs.aws.amazon.com/AmazonS3/latest/dev/SSEUsingRESTAPI.html
	// For SSE-C  see: https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
	SSEAlgorithmAES256 = "AES256"
)

// RemoveSensitiveHeaders removes confidential encryption
// information - e.g. the SSE-C key - from the HTTP headers.
// It has the same semantics as RemoveSensitiveEntires.
func RemoveSensitiveHeaders(h http.Header) {
	h.Del(SSECKey)
	h.Del(SSECopyKey)
}

// S3 represents AWS SSE-S3. It provides functionality to handle
// SSE-S3 requests.
var S3 = s3{}

type s3 struct{}

// IsRequested returns true if the HTTP headers indicates that
// the S3 client requests SSE-S3.
func (s3) IsRequested(h http.Header, brand Brand) bool {
	_, ok := h[brand.GetGeneralFieldFullName(XServerSideEncryption)]
	return ok && strings.ToLower(h.Get(brand.GetGeneralFieldFullName(XServerSideEncryption))) != brand.GetSpecialFieldFullName(SSEAlgorithmKMS) // Return only true if the SSE header is specified and does not contain the SSE-KMS value
}

// ParseHTTP parses the SSE-S3 related HTTP headers and checks
// whether they contain valid values.
func (s3) ParseHTTP(h http.Header, brand Brand) (err error) {
	if h.Get(brand.GetGeneralFieldFullName(XServerSideEncryption)) != SSEAlgorithmAES256 {
		err = ErrInvalidEncryptionMethod
	}
	return
}

// S3KMS represents AWS SSE-KMS. It provides functionality to
// handle SSE-KMS requests.
var S3KMS = s3KMS{}

type s3KMS struct{}

// IsRequested returns true if the HTTP headers indicates that
// the S3 client requests SSE-KMS.
func (s3KMS) IsRequested(h http.Header, brand Brand) bool {
	if _, ok := h[brand.GetSpecialFieldFullName(SSEKmsID)]; ok {
		return true
	}
	if _, ok := h[brand.GetGeneralFieldFullName(XSSEKmsContext)]; ok {
		return true
	}
	if _, ok := h[brand.GetGeneralFieldFullName(XServerSideEncryption)]; ok {
		return strings.ToUpper(h.Get(brand.GetGeneralFieldFullName(XServerSideEncryption))) != SSEAlgorithmAES256 // Return only true if the SSE header is specified and does not contain the SSE-S3 value
	}
	return false
}

var (
	// SSEC represents AWS SSE-C. It provides functionality to handle
	// SSE-C requests.
	SSEC = ssec{}

	// SSECopy represents AWS SSE-C for copy requests. It provides
	// functionality to handle SSE-C copy requests.
	SSECopy = ssecCopy{}
)

type ssec struct{}
type ssecCopy struct{}

// IsRequested returns true if the HTTP headers contains
// at least one SSE-C header. SSE-C copy headers are ignored.
func (ssec) IsRequested(h http.Header, brand Brand) bool {
	if _, ok := h[brand.GetGeneralFieldFullName(XSSECAlgorithm)]; ok {
		return true
	}
	if _, ok := h[brand.GetGeneralFieldFullName(XSSECKey)]; ok {
		return true
	}
	if _, ok := h[brand.GetGeneralFieldFullName(XSSECKeyMD5)]; ok {
		return true
	}
	return false
}

// IsRequested returns true if the HTTP headers contains
// at least one SSE-C copy header. Regular SSE-C headers
// are ignored.
func (ssecCopy) IsRequested(h http.Header, brand Brand) bool {
	if _, ok := h[brand.GetGeneralFieldFullName(XSSECopyAlgorithm)]; ok {
		return true
	}
	if _, ok := h[brand.GetGeneralFieldFullName(XSSECopyKey)]; ok {
		return true
	}
	if _, ok := h[brand.GetGeneralFieldFullName(XSSECopyKeyMD5)]; ok {
		return true
	}
	return false
}

// ParseHTTP parses the SSE-C headers and returns the SSE-C client key
// on success. SSE-C copy headers are ignored.
func (ssec) ParseHTTP(h http.Header, brand Brand) (key [32]byte, err error) {
	if h.Get(brand.GetGeneralFieldFullName(XSSECAlgorithm)) != SSEAlgorithmAES256 {
		return key, ErrInvalidCustomerAlgorithm
	}
	if h.Get(brand.GetGeneralFieldFullName(XSSECKey)) == "" {
		return key, ErrMissingCustomerKey
	}
	if h.Get(brand.GetGeneralFieldFullName(XSSECKeyMD5)) == "" {
		return key, ErrMissingCustomerKeyMD5
	}

	clientKey, err := base64.StdEncoding.DecodeString(h.Get(brand.GetGeneralFieldFullName(XSSECKey)))
	if err != nil || len(clientKey) != 32 { // The client key must be 256 bits long
		return key, ErrInvalidCustomerKey
	}
	keyMD5, err := base64.StdEncoding.DecodeString(h.Get(brand.GetGeneralFieldFullName(XSSECKeyMD5)))
	if md5Sum := md5.Sum(clientKey); err != nil || !bytes.Equal(md5Sum[:], keyMD5) {
		return key, ErrCustomerKeyMD5Mismatch
	}
	copy(key[:], clientKey)
	return key, nil
}

// ParseHTTP parses the SSE-C copy headers and returns the SSE-C client key
// on success. Regular SSE-C headers are ignored.
func (ssecCopy) ParseHTTP(h http.Header, brand Brand) (key [32]byte, err error) {
	if h.Get(brand.GetGeneralFieldFullName(XSSECopyAlgorithm)) != SSEAlgorithmAES256 {
		return key, ErrInvalidCustomerAlgorithm
	}
	if h.Get(brand.GetGeneralFieldFullName(XSSECopyKey)) == "" {
		return key, ErrMissingCustomerKey
	}
	if h.Get(brand.GetGeneralFieldFullName(XSSECopyKeyMD5)) == "" {
		return key, ErrMissingCustomerKeyMD5
	}

	clientKey, err := base64.StdEncoding.DecodeString(h.Get(brand.GetGeneralFieldFullName(XSSECopyKey)))
	if err != nil || len(clientKey) != 32 { // The client key must be 256 bits long
		return key, ErrInvalidCustomerKey
	}
	keyMD5, err := base64.StdEncoding.DecodeString(h.Get(brand.GetGeneralFieldFullName(XSSECopyKeyMD5)))
	if md5Sum := md5.Sum(clientKey); err != nil || !bytes.Equal(md5Sum[:], keyMD5) {
		return key, ErrCustomerKeyMD5Mismatch
	}
	copy(key[:], clientKey)
	return key, nil
}
