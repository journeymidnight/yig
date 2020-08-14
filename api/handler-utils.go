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
	. "github.com/journeymidnight/yig/brand"
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
	// Add more supported headers here
}

// extractMetadataFromHeader extracts metadata from HTTP header.
func extractMetadataFromHeader(header http.Header, brandName Brand) map[string]string {
	metadata := make(map[string]string)
	// Save standard supported headers.
	for _, supportedHeader := range supportedHeaders {
		if value, ok := header[http.CanonicalHeaderKey(supportedHeader)]; ok {
			metadata[http.CanonicalHeaderKey(supportedHeader)] = value[0]
		} else if value, ok := header[supportedHeader]; ok {
			metadata[supportedHeader] = value[0]
		}
	}
	// Go through all other headers for any additional headers that needs to be saved.
	for key := range header {
		if strings.HasPrefix(strings.ToLower(key), strings.ToLower(brandName.GetGeneralFieldFullName(XMeta))+"-") {
			value, ok := header[key]
			if ok {
				metadata[key] = strings.Join(value, ",")
				break
			}
		}
	}
	// Return.
	return metadata
}

func parseSseHeader(header http.Header, brandName Brand) (request SseRequest, err error) {
	// sse three options are mutually exclusive
	if crypto.S3.IsRequested(header, brandName) && crypto.SSEC.IsRequested(header, brandName) {
		return request, ErrIncompatibleEncryptionMethod
	}

	if sse := header.Get(brandName.GetGeneralFieldFullName(XServerSideEncryption)); sse != "" {
		switch sse {
		case strings.ToLower(brandName.GetSpecialFieldFullName(SSEAlgorithmKMS)):
			err = ErrNotImplemented
			return request, err
		case crypto.SSEAlgorithmAES256:
			request.Type = crypto.S3.String()
		default:
			err = ErrInvalidSseHeader
			return request, err
		}
	}

	if sse := header.Get(brandName.GetGeneralFieldFullName(XSSECAlgorithm)); sse != "" {
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
		key, err := crypto.SSEC.ParseHTTP(header, brandName)
		if err != nil {
			return request, err
		}
		request.SseCustomerAlgorithm = crypto.SSEAlgorithmAES256
		request.SseCustomerKey = key[:]
		return request, nil
	}

	// SSECCopy not support now.
	if sse := header.Get(brandName.GetGeneralFieldFullName(XSSECopyAlgorithm)); sse != "" {
		if sse != crypto.SSEAlgorithmAES256 {
			err = ErrInvalidSseHeader
			return
		}
		request.CopySourceSseCustomerAlgorithm = sse
		key := header.Get(brandName.GetGeneralFieldFullName(XSSECopyKey))
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
		userMd5 := header.Get(brandName.GetGeneralFieldFullName(XSSECopyKeyMD5))
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
