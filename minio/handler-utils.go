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

package minio

import (
	. "git.letv.cn/yig/yig/minio/datatype"
	"io"
	"net/http"
	"strings"
	. "git.letv.cn/yig/yig/error"
)

// validates location constraint from the request body.
// the location value in the request body should match the Region in serverConfig.
// other values of location are not accepted.
// make bucket fails in such cases.
func isValidLocationContraint(reqBody io.Reader, serverRegion string) error {
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
		if locationContraint.Location != "" && serverRegion != locationContraint.Location {
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
