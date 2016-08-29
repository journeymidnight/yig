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
	"net/url"
	"strconv"

	. "git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/helper"
)

func parseListObjectsQuery(query url.Values) (request ListObjectsRequest, err error) {
	if query.Get("list-type") == "2" {
		request.Version = 2
		request.ContinuationToken = query.Get("continuation-token")
		request.StartAfter = query.Get("start-after")
		request.FetchOwner = helper.Ternary(query.Get("fetch-owner") == "true",
			true, false).(bool)
	} else {
		request.Version = 1
		request.Marker = query.Get("marker")
	}
	request.Delimiter = query.Get("delimiter")
	request.EncodingType = query.Get("encoding-type")
	if request.EncodingType != "" && request.EncodingType != "url" {
		err = ErrInvalidEncodingType
		return
	}
	if query.Get("max-keys") == "" {
		request.MaxKeys = MaxObjectList
	} else {
		maxKeys, err := strconv.Atoi(query.Get("max-keys"))
		if err != nil {
			return request, err
		}
		if maxKeys > MaxObjectList || maxKeys < 1 {
			err = ErrInvalidMaxKeys
			return
		}
		request.MaxKeys = maxKeys
	}
	request.Prefix = query.Get("prefix")
	return
}

// Parse bucket url queries for ?uploads
func getBucketMultipartResources(values url.Values) (prefix, keyMarker, uploadIDMarker, delimiter string,
	maxUploads int, encodingType string) {
	prefix = values.Get("prefix")
	keyMarker = values.Get("key-marker")
	uploadIDMarker = values.Get("upload-id-marker")
	delimiter = values.Get("delimiter")
	if values.Get("max-uploads") != "" {
		maxUploads, _ = strconv.Atoi(values.Get("max-uploads"))
	} else {
		maxUploads = MaxUploadsList
	}
	encodingType = values.Get("encoding-type")
	return
}

// Parse object url queries
func getObjectResources(values url.Values) (uploadID string, partNumberMarker, maxParts int,
	encodingType string) {
	uploadID = values.Get("uploadId")
	partNumberMarker, _ = strconv.Atoi(values.Get("part-number-marker"))
	if values.Get("max-parts") != "" {
		maxParts, _ = strconv.Atoi(values.Get("max-parts"))
	} else {
		maxParts = MaxPartsList
	}
	encodingType = values.Get("encoding-type")
	return
}
