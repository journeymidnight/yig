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
	"github.com/journeymidnight/yig/helper"
	meta "github.com/journeymidnight/yig/meta/types"
)

// Refer: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
var CommonS3ResponseHeaders = []string{"Content-Length", "Content-Type", "Connection", "Date", "ETag", "Server",
	"x-amz-delete-marker", "x-amz-id-2", "x-amz-request-id", "x-amz-version-id"}

// Encodes the response headers into XML format.
func EncodeResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	e := xml.NewEncoder(&bytesBuffer)
	err := e.Encode(response)
	if err != nil {
		helper.Logger.Error("Encode XML err:", err)
	}
	return bytesBuffer.Bytes()
}

// Write object header
func SetObjectHeaders(w http.ResponseWriter, object *meta.Object, contentRange *HttpRange, statusCode int) {
	// set object-related metadata headers
	lastModified := object.LastModifiedTime.UTC().Format(http.TimeFormat)
	w.Header().Set("Last-Modified", lastModified)

	w.Header().Set("Content-Type", object.ContentType)
	if object.Etag != "" {
		//	w.Header().Set("ETag", "\""+object.Etag+"\"")
		w.Header()["ETag"] = []string{"\"" + object.Etag + "\""}
	}

	for key, val := range object.CustomAttributes {
		w.Header().Set(key, val)
	}
	//default cache-control is no-store
	if _, ok := object.CustomAttributes["Cache-Control"]; !ok {
		w.Header().Set("Cache-Control", "no-store")
	}

	w.Header().Set("X-Amz-Object-Type", object.ObjectTypeToString())
	w.Header().Set("X-Amz-Storage-Class", object.StorageClass.ToString())
	w.Header().Set("Content-Length", strconv.FormatInt(object.Size, 10))
	if object.Type == meta.ObjectTypeAppendable {
		w.Header().Set("X-Amz-Next-Append-Position", strconv.FormatInt(object.Size, 10))
	}

	// for providing ranged content
	if contentRange != nil && contentRange.OffsetBegin > -1 {
		// Override content-length
		w.Header().Set("Content-Length", strconv.FormatInt(contentRange.GetLength(), 10))
		w.Header().Set("Content-Range", contentRange.String())
		w.WriteHeader(http.StatusPartialContent)
	}

	if object.VersionId != meta.NullVersion {
		w.Header()["x-amz-version-id"] = []string{object.VersionId}
	}

	if object.DeleteMarker {
		w.Header()["x-amz-delete-marker"] = []string{"true"}
		statusCode = http.StatusNotFound
	}

	w.WriteHeader(statusCode)
}
