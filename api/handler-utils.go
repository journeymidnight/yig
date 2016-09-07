/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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

package api

import (
	"crypto/md5"
	"encoding/base64"
	. "git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"io"
	"net/http"
	"strings"
)

const (
	REGION = "cn-bj-1"
)

// validates location constraint from the request body.
// the location value in the request body should match the Region in serverConfig.
// other values of location are not accepted.
// make bucket fails in such cases.
func isValidLocationContraint(reqBody io.Reader) error {
	var locationContraint CreateBucketLocationConfiguration
	var errCode error
	errCode = nil
	e := xmlDecoder(reqBody, &locationContraint)
	if e != nil {
		if e == io.EOF {
			// Do nothing.
			// failed due to empty body. The location will be set to default value from the serverConfig.
			// this is valid.
			errCode = nil
		} else {
			// Failed due to malformed configuration.
			errCode = ErrMalformedXML
			//WriteErrorResponse(w, r, ErrMalformedXML, r.URL.Path)
		}
	} else {
		// Region obtained from the body.
		// It should be equal to Region in serverConfig.
		// Else ErrInvalidRegion returned.
		// For empty value location will be to set to  default value from the serverConfig.
		if locationContraint.Location != "" && REGION != locationContraint.Location {
			//WriteErrorResponse(w, r, ErrInvalidRegion, r.URL.Path)
			errCode = ErrInvalidRegion
		}
	}
	return errCode
}

// Supported headers that needs to be extracted.
var supportedHeaders = []string{
	"Content-Type",
	"Cache-Control",
	"Content-Encoding",
	"Content-Disposition",
	// Add more supported headers here, in "canonical" form
}

// extractMetadataFromHeader extracts metadata from HTTP header.
func extractMetadataFromHeader(header http.Header) map[string]string {
	metadata := make(map[string]string)
	// Save standard supported headers.
	for _, supportedHeader := range supportedHeaders {
		if h := header.Get(supportedHeader); h != "" {
			metadata[supportedHeader] = h
		}
	}
	// Go through all other headers for any additional headers that needs to be saved.
	for key := range header {
		if strings.HasPrefix(key, "X-Amz-Meta-") {
			metadata[key] = header.Get(key)
		}
	}
	// Return.
	return metadata
}

func parseSseHeader(header http.Header) (request SseRequest, err error) {
	if sse := header.Get("X-Amz-Server-Side-Encryption"); sse != "" {
		switch sse {
		case "aws:kms":
			err = ErrNotImplemented
			return
		case "AES256":
			request.Type = "S3"
		default:
			err = ErrInvalidSseHeader
			return
		}
	}
	if sse := header.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm"); sse != "" {
		if sse == "AES256" {
			request.Type = "C"
		} else {
			err = ErrInvalidSseHeader
			return
		}
	}

	switch request.Type {
	case "KMS":
		break // Not implemented yet
	case "S3":
		request.SseContext = header.Get("X-Amz-Server-Side-Encryption-Context")
	case "C":
		request.SseCustomerAlgorithm = header.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm")
		if request.SseCustomerAlgorithm != "AES256" {
			err = ErrInvalidSseHeader
			return
		}
		// base64-encoded encryption key
		key := header.Get("X-Amz-Server-Side-Encryption-Customer-Key")
		n, err := base64.StdEncoding.Decode(request.SseCustomerKey, byte(key))
		if err != nil {
			return
		}
		if n != 32 { // Should be 32 bytes for AES-"256"
			err = ErrInvalidSseHeader
			return
		}
		// base64-encoded 128-bit MD5 digest of the encryption key
		userMd5 := header.Get("X-Amz-Server-Side-Encryption-Customer-Key-Md5")
		calculatedMd5 := md5.Sum(request.SseCustomerKey)
		encodedMd5 := base64.StdEncoding.EncodeToString(calculatedMd5)
		if userMd5 != encodedMd5 {
			err = ErrInvalidSseHeader
			return
		}
	}
	return
}
