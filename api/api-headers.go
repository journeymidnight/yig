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

package api

import (
	"bytes"
	"crypto/rand"
	"encoding/xml"
	. "git.letv.cn/yig/yig/api/datatype"
	"git.letv.cn/yig/yig/meta"
	"net/http"
	"strconv"
)

//// helpers

// Static alphaNumeric table used for generating unique request ids
var alphaNumericTable = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

// generateRequestID - Generate request id
func generateRequestID() []byte {
	alpha := make([]byte, 16)
	rand.Read(alpha)
	for i := 0; i < 16; i++ {
		alpha[i] = alphaNumericTable[alpha[i]%byte(len(alphaNumericTable))]
	}
	return alpha
}

// Write http common headers
func SetCommonHeaders(w http.ResponseWriter) {
	// Set unique request ID for each reply.
	w.Header().Set("X-Amz-Request-Id", string(generateRequestID()))
	w.Header().Set("Server", "LeCloud YIG")
	w.Header().Set("Accept-Ranges", "bytes")
}

// Encodes the response headers into XML format.
func EncodeResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	e := xml.NewEncoder(&bytesBuffer)
	e.Encode(response)
	return bytesBuffer.Bytes()
}

// Write object header
func SetObjectHeaders(w http.ResponseWriter, object *meta.Object, contentRange *HttpRange) {
	// set common headers
	SetCommonHeaders(w)

	// set object-related metadata headers
	lastModified := object.LastModifiedTime.UTC().Format(http.TimeFormat)
	w.Header().Set("Last-Modified", lastModified)

	w.Header().Set("Content-Type", object.ContentType)
	if object.Etag != "" {
		w.Header().Set("ETag", "\""+object.Etag+"\"")
	}

	w.Header().Set("Content-Length", strconv.FormatInt(object.Size, 10))

	// for providing ranged content
	if contentRange != nil && contentRange.OffsetBegin > -1 {
		// Override content-length
		w.Header().Set("Content-Length", strconv.FormatInt(contentRange.GetLength(), 10))
		w.Header().Set("Content-Range", contentRange.String())
		w.WriteHeader(http.StatusPartialContent)
	}
}
