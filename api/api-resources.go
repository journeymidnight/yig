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

	"unicode/utf8"

	. "github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
)

func parseListObjectsQuery(query url.Values) (request ListObjectsRequest, err error) {
	if query.Get("list-type") == "2" {
		request.Version = 2
		request.ContinuationToken = query.Get("continuation-token")
		request.StartAfter = query.Get("start-after")
		if !utf8.ValidString(request.StartAfter) {
			err = ErrNonUTF8Encode
			return
		}
		request.FetchOwner = helper.Ternary(query.Get("fetch-owner") == "true",
			true, false).(bool)
	} else {
		request.Version = 1
		request.Marker = query.Get("marker")
		if !utf8.ValidString(request.Marker) {
			err = ErrNonUTF8Encode
			return
		}
	}
	request.Delimiter = query.Get("delimiter")
	if !utf8.ValidString(request.Delimiter) {
		err = ErrNonUTF8Encode
		return
	}
	request.EncodingType = query.Get("encoding-type")
	if request.EncodingType != "" && request.EncodingType != "url" {
		err = ErrInvalidEncodingType
		return
	}
	if query.Get("max-keys") == "" {
		request.MaxKeys = MaxObjectList
	} else {
		request.MaxKeys, err = strconv.Atoi(query.Get("max-keys"))
		if err != nil {
			helper.Logger.Error("Error parsing max-keys:", err)
			return request, ErrInvalidMaxKeys
		}
		if request.MaxKeys > MaxObjectList || request.MaxKeys < 1 {
			err = ErrInvalidMaxKeys
			return
		}
	}
	request.Prefix = query.Get("prefix")
	if !utf8.ValidString(request.Prefix) {
		err = ErrNonUTF8Encode
		return
	}

	request.KeyMarker = query.Get("key-marker")
	if !utf8.ValidString(request.KeyMarker) {
		err = ErrNonUTF8Encode
		return
	}
	request.VersionIdMarker = query.Get("version-id-marker")
	if !utf8.ValidString(request.VersionIdMarker) {
		err = ErrNonUTF8Encode
		return
	}

	if request.KeyMarker == "" && request.VersionIdMarker != "" {
		err = ErrInvalidVersioning
		return
	}

	return
}

// Parse bucket url queries for ?uploads
func parseListUploadsQuery(query url.Values) (request ListUploadsRequest, err error) {
	request.Delimiter = query.Get("delimiter")
	request.EncodingType = query.Get("encoding-type")
	if query.Get("max-uploads") == "" {
		request.MaxUploads = MaxUploadsList
	} else {
		request.MaxUploads, err = strconv.Atoi(query.Get("max-uploads"))
		if err != nil {
			return
		}
		if request.MaxUploads > MaxUploadsList || request.MaxUploads < 1 {
			err = ErrInvalidMaxUploads
			return
		}
	}
	request.KeyMarker = query.Get("key-marker")
	request.Prefix = query.Get("prefix")
	request.UploadIdMarker = query.Get("upload-id-marker")
	return
}

// Parse object url queries
func parseListObjectPartsQuery(query url.Values) (request ListPartsRequest, err error) {
	request.EncodingType = query.Get("encoding-type")
	request.UploadId = query.Get("uploadId")
	if request.UploadId == "" {
		err = ErrNoSuchUpload
		return
	}
	if query.Get("max-parts") == "" {
		request.MaxParts = MaxPartsList
	} else {
		request.MaxParts, err = strconv.Atoi(query.Get("max-parts"))
		if err != nil {
			return
		}
		if request.MaxParts > MaxPartsList || request.MaxParts < 1 {
			err = ErrInvalidMaxParts
			return
		}
	}
	if query.Get("part-number-marker") != "" {
		request.PartNumberMarker, err = strconv.Atoi(query.Get("part-number-marker"))
		if err != nil {
			err = ErrInvalidPartNumberMarker
			return
		}
		if request.PartNumberMarker < 0 {
			err = ErrInvalidPartNumberMarker
			return
		}
	}
	return
}
