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

	. "github.com/journeymidnight/yig/brand"
	"github.com/journeymidnight/yig/helper"
	meta "github.com/journeymidnight/yig/meta/types"
)

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
func SetObjectHeaders(w http.ResponseWriter, object *meta.Object, brand Brand) {
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

	if object.Parts != nil {
		w.Header().Set(brand.GetHeaderFieldKey(XMpPartsCount), strconv.Itoa(len(object.Parts)))
	}
	w.Header().Set(brand.GetHeaderFieldKey(XObjectType), object.ObjectTypeToString())
	w.Header().Set(brand.GetHeaderFieldKey(XStorageClass), object.StorageClass.ToString())
	w.Header().Set("Content-Length", strconv.FormatInt(object.Size, 10))
	if object.Type == meta.ObjectTypeAppendable {
		w.Header().Set(brand.GetHeaderFieldKey(XNextAppendPosition), strconv.FormatInt(object.Size, 10))
	}

	if object.VersionId != meta.NullVersion {
		w.Header()[brand.GetHeaderFieldKey(XVersionId)] = []string{object.VersionId}
	}

	if object.DeleteMarker {
		w.Header()[brand.GetHeaderFieldKey(XDeleteMarker)] = []string{"true"}
	}
}
