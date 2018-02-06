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
	"encoding/xml"
	"net/http"
	"strconv"

	. "github.com/journeymidnight/yig/api/datatype"
	meta "github.com/journeymidnight/yig/meta/types"
)

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
	// set object-related metadata headers
	lastModified := object.LastModifiedTime.UTC().Format(http.TimeFormat)
	w.Header().Set("Last-Modified", lastModified)

	w.Header().Set("Content-Type", object.ContentType)
	if object.Etag != "" {
		//	w.Header().Set("ETag", "\""+object.Etag+"\"")
		w.Header()["ETag"] = []string{"\"" + object.Etag + "\""}
	}

	var existCacheControl bool
	for key, val := range object.CustomAttributes {
		if key == "Cache-Control" {
			existCacheControl = true
		}
		w.Header().Set(key, val)
	}
	if !existCacheControl {
		w.Header().Set("Cache-Control", "public, max-age=30672000")
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
