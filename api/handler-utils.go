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
	"io"
	"net/http"
	"strings"

	. "github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/crypto"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
)

// validates location constraint from the request body.
// the location value in the request body should match the Region in serverConfig.
// other values of location are not accepted.
// make bucket fails in such cases.
func isValidLocationConstraint(reqBody io.Reader) (err error) {
	var region = helper.CONFIG.Region
	var locationConstraint CreateBucketLocationConfiguration
	e := xmlDecoder(reqBody, &locationConstraint)
	if e != nil {
		if e == io.EOF {
			// Failed due to empty request body. The location will be set to
			// default value from the serverConfig
			err = nil
		} else {
			// Failed due to malformed configuration.
			err = ErrMalformedXML
		}
	} else {
		// Region obtained from the body.
		// It should be equal to Region in serverConfig.
		// Else ErrInvalidRegion returned.
		// For empty value location will be to set to  default value from the serverConfig.
		if locationConstraint.Location != "" && region != locationConstraint.Location {
			err = ErrInvalidRegion
		}
	}
	return err
}

// Supported headers that needs to be extracted.
var supportedHeaders = []string{
	"cache-control",
	"content-disposition",
	"content-encoding",
	"content-language",
	"content-type",
	"expires",
	"website-redirect-location",
	// Add more supported headers here, in "canonical" form
}

// extractMetadataFromHeader extracts metadata from HTTP header.
func extractMetadataFromHeader(header http.Header) map[string]string {
	metadata := make(map[string]string)
	// Save standard supported headers.
	for _, supportedHeader := range supportedHeaders {
		if h := header.Get(http.CanonicalHeaderKey(supportedHeader)); h != "" {
			metadata[supportedHeader] = h
		}
	}
	// Go through all other headers for any additional headers that needs to be saved.
	for key := range header {
		if strings.HasPrefix(strings.ToLower(key), "x-amz-meta-") {
			metadata[key] = header.Get(key)
		}
	}
	// Return.
	return metadata
}

func parseSseHeader(header http.Header) (request SseRequest, err error) {
	// sse three options are mutually exclusive
	if crypto.S3.IsRequested(header) && crypto.SSEC.IsRequested(header) {
		return request, ErrIncompatibleEncryptionMethod
	}

	if sse := header.Get(crypto.SSEHeader); sse != "" {
		switch sse {
		case crypto.SSEAlgorithmKMS:
			err = ErrNotImplemented
			return request, err
		case crypto.SSEAlgorithmAES256:
			request.Type = crypto.S3.String()
		default:
			err = ErrInvalidSseHeader
			return request, err
		}
	}

	if sse := header.Get(crypto.SSECAlgorithm); sse != "" {
		if sse == crypto.SSEAlgorithmAES256 {
			request.Type = crypto.SSEC.String()
		} else {
			err = ErrInvalidSseHeader
			return
		}
	}

	switch request.Type {
	case crypto.S3KMS.String():
		// Not implemented yet
		return request, ErrNotImplemented
	case crypto.S3.String():
		// encrypt key will retrieve from kms now
		return request, nil
	case crypto.SSEC.String():
		// validate ssec header
		key, err := crypto.SSEC.ParseHTTP(header)
		if err != nil {
			return request, err
		}
		request.SseCustomerAlgorithm = crypto.SSEAlgorithmAES256
		request.SseCustomerKey = key[:]
		return request, nil
	}

	// SSECCopy not support now.
	if sse := header.Get(crypto.SSECopyAlgorithm); sse != "" {
		if sse != crypto.SSEAlgorithmAES256 {
			err = ErrInvalidSseHeader
			return
		}
		request.CopySourceSseCustomerAlgorithm = sse
		key := header.Get(crypto.SSECopyKey)
		if key == "" {
			err = ErrInvalidSseHeader
			return
		}
		request.CopySourceSseCustomerKey = make([]byte, len(key))
		var n int
		n, err = base64.StdEncoding.Decode(request.CopySourceSseCustomerKey, []byte(key))
		if err != nil {
			return
		}
		if n != 32 {
			err = ErrInvalidSseHeader
			return
		}
		request.CopySourceSseCustomerKey = request.CopySourceSseCustomerKey[:32]
		userMd5 := header.Get(crypto.SSECopyKeyMD5)
		if userMd5 == "" {
			err = ErrInvalidSseHeader
			return
		}
		calculatedMd5 := md5.Sum(request.CopySourceSseCustomerKey)
		encodedMd5 := base64.StdEncoding.EncodeToString(calculatedMd5[:])
		if userMd5 != encodedMd5 {
			err = ErrInvalidSseHeader
			return
		}
	}
	return
}

// Suffix matcher string matches suffix in a platform specific way.
// For example on windows since its case insensitive we are supposed
// to do case insensitive checks.
func hasSuffix(s string, suffix string) bool {
	return strings.HasSuffix(s, suffix)
}
