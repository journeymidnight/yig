// Copyright 2015 Light Code Labs, LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/journeymidnight/yig/meta/common"

	. "github.com/journeymidnight/yig/context"
	"github.com/journeymidnight/yig/helper"
)

const (
	CombinedLogFormat = "{time_local} {request_uri} {request_id} {operation_name} {host_name} {bucket_name} {object_name} " +
		"{object_size} {requester_id} {project_id} {remote_addr} {http_x_real_ip} {request_length} {server_cost} " +
		"{request_time} {http_status} {error_code} {body_bytes_sent} {http_referer} {http_user_agent}"

	BillingLogFormat = "{is_private_subnet} {storage_class} {target_storage_class} {bucket_logging} {cdn_request} {region_id} {create_time} {delta_size}"
)

// Replacer is a type which can replace placeholder
// substrings in a string with actual values from a
// http.Request and ResponseRecorder. Always use
// NewReplacer to get one of these. Any placeholders
// made with Set() should overwrite existing values if
// the key is already used.
type Replacer interface {
	Replace(string) string
	Set(key, value string)
	GetReplacedValues() map[string]string
}

// replacer implements Replacer. customReplacements
// is used to store custom replacements created with
// Set() until the time of replacement, at which point
// they will be used to overwrite other replacements
// if there is a name conflict.
type replacer struct {
	customReplacements map[string]string
	emptyValue         string
	responseRecorder   *ResponseRecorder
	request            *http.Request
	replacedValues     map[string]string
}

// NewReplacer makes a new replacer based on r and rr which
// are used for request and response placeholders, respectively.
// Request placeholders are created immediately, whereas
// response placeholders are not created until Replace()
// is invoked. rr may be nil if it is not available.
// emptyValue should be the string that is used in place
// of empty string (can still be empty string).
func NewReplacer(r *http.Request, rr *ResponseRecorder, emptyValue string) Replacer {
	rep := &replacer{
		request:          r,
		responseRecorder: rr,
		emptyValue:       emptyValue,
		replacedValues:   make(map[string]string),
	}

	return rep
}

// unescapeBraces finds escaped braces in s and returns
// a string with those braces unescaped.
func unescapeBraces(s string) string {
	s = strings.Replace(s, "\\{", "{", -1)
	s = strings.Replace(s, "\\}", "}", -1)
	return s
}

// Replace performs a replacement of values on s and returns
// the string with the replaced values.
func (r *replacer) Replace(s string) string {
	// Do not attempt replacements if no placeholder is found.
	if !strings.ContainsAny(s, "{}") {
		return s
	}

	result := ""
Placeholders: // process each placeholder in sequence
	for {
		var idxStart, idxEnd int

		idxOffset := 0
		for { // find first unescaped opening brace
			searchSpace := s[idxOffset:]
			idxStart = strings.Index(searchSpace, "{")
			if idxStart == -1 {
				// no more placeholders
				break Placeholders
			}
			if idxStart == 0 || searchSpace[idxStart-1] != '\\' {
				// preceding character is not an escape
				idxStart += idxOffset
				break
			}
			// the brace we found was escaped
			// search the rest of the string next
			idxOffset += idxStart + 1
		}

		idxOffset = 0
		for { // find first unescaped closing brace
			searchSpace := s[idxStart+idxOffset:]
			idxEnd = strings.Index(searchSpace, "}")
			if idxEnd == -1 {
				// unpaired placeholder
				break Placeholders
			}
			if idxEnd == 0 || searchSpace[idxEnd-1] != '\\' {
				// preceding character is not an escape
				idxEnd += idxOffset + idxStart
				break
			}
			// the brace we found was escaped
			// search the rest of the string next
			idxOffset += idxEnd + 1
		}

		// get a replacement for the unescaped placeholder
		placeholder := unescapeBraces(s[idxStart : idxEnd+1])
		replacement := r.getSubstitution(placeholder)
		if replacement == "" {
			replacement = "-"
		}
		r.setReplacedValue(placeholder, replacement)

		// append unescaped prefix + replacement
		result += strings.TrimPrefix(unescapeBraces(s[:idxStart]), "\\") + replacement

		// strip out scanned parts
		s = s[idxEnd+1:]
	}

	// append unscanned parts
	return result + unescapeBraces(s)
}

func (r *replacer) GetReplacedValues() map[string]string {
	return r.replacedValues
}

func (r *replacer) setReplacedValue(key, value string) {
	name := strings.Replace(key, "{", "", -1)
	name = strings.Replace(name, "}", "", -1)
	r.replacedValues[name] = value
}

// getSubstitution retrieves value from corresponding key
func (r *replacer) getSubstitution(key string) string {

	// search default replacements in the end
	switch key {
	case "{time_local}":
		timeLocal := time.Now().Format("2006-01-02 15:04:05")
		return "[" + timeLocal + "]"
	case "{request_uri}":
		return r.request.Method + " " + r.request.URL.String() + " " + r.request.Proto
	case "{request_id}":
		return GetRequestContext(r.request).RequestID
	case "{operation_name}":
		if r.responseRecorder.operationName == "" {
			return "-"
		}
		return string(r.responseRecorder.operationName)
	case "{host_name}":
		return r.request.Host
	case "{region_id}":
		if helper.CONFIG.Region == "" {
			return "-"
		}
		return helper.CONFIG.Region
	case "{bucket_name}":
		bucketName := GetRequestContext(r.request).BucketName
		if bucketName == "" {
			return "-"
		}
		return bucketName
	case "{object_name}":
		objectName := GetRequestContext(r.request).ObjectName
		if objectName == "" {
			return "-"
		}
		return objectName
	case "{object_size}":
		var objectSize int64
		objectInfo := GetRequestContext(r.request).ObjectInfo
		if objectInfo != nil {
			objectSize = objectInfo.Size
		}
		return strconv.FormatInt(objectSize, 10)
	case "{requester_id}":
		requester_id := "-"
		// TODO: add requester_id
		return requester_id
	case "{project_id}":
		bucketInfo := GetRequestContext(r.request).BucketInfo
		if bucketInfo == nil {
			return "-"
		}
		return bucketInfo.OwnerId
	case "{remote_addr}":
		return r.request.RemoteAddr
	case "{http_x_real_ip}":
		if realIP := r.request.Header.Get("X-Real-Ip"); realIP != "" {
			return realIP
		}
		return "-"
	case "{request_length}":
		requestLength := r.request.ContentLength
		result := strconv.FormatInt(requestLength, 10)
		return result
	case "{server_cost}":
		// TODO
		fallthrough
	case "{request_time}":
		requestTime := r.responseRecorder.requestTime
		//Duration convert to Millisecond
		temp := requestTime.Nanoseconds() / 1e6
		result := strconv.FormatInt(temp, 10)
		return result
	case "{http_status}":
		status := r.responseRecorder.status
		result := strconv.Itoa(status)
		return result
	case "{error_code}":
		if r.responseRecorder.errorCode == "" {
			return "-"
		}
		return r.responseRecorder.errorCode
	case "{body_bytes_sent}":
		bodyBytesSent := r.responseRecorder.size
		result := strconv.FormatInt(bodyBytesSent, 10)
		return result
	case "{http_user_agent}":
		if agent := r.request.Header.Get("User-Agent"); agent != "" {
			return "\"" + agent + "\""
		}
		return `"-"`
	case "{retain}":
		return "-"
	case "{http_referer}":
		//see https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Referer
		if referer := r.request.Header.Get("Referer"); referer != "" {
			return "\"" + referer + "\""
		}
		return `"-"`

		// Billing labels
	case "{is_private_subnet}":
		// Currently, the intranet domain name is formed by adding the "-internal" on the second-level domain name of the public network.
		return strconv.FormatBool(strings.Contains(r.request.Host, "internal"))
	case "{storage_class}":
		objectInfo := GetRequestContext(r.request).ObjectInfo
		if objectInfo == nil {
			return "-"
		}
		return objectInfo.StorageClass.ToString()
	case "{target_storage_class}":
		if r.request.Method == http.MethodPut || r.request.Method == http.MethodPost {
			storageClassFromHeader, err := getStorageClassFromHeader(r.request.Header)
			if err != nil {
				return "-"
			}
			return storageClassFromHeader.ToString()
		}
		return "-"
	case "{bucket_logging}":
		bl := GetRequestContext(r.request).BucketInfo
		if bl != nil {
			if bl.BucketLogging.SetLog == true {
				return strconv.FormatBool(true)
			}
		}
		return strconv.FormatBool(false)
	case "{cdn_request}":
		// TODO: change to go plugin
		var judgeFunc JudgeCdnRequest
		judgeFunc = judgeCdnRequestFromQuery
		return strconv.FormatBool(judgeFunc(r.request))
	case "{create_time}":
		objectInfo := GetRequestContext(r.request).ObjectInfo
		if objectInfo == nil {
			return "-"
		}
		return strconv.FormatUint(objectInfo.CreateTime, 10)
	case "{delta_size}":
		return strconv.FormatInt(r.responseRecorder.deltaSizeInfo[common.ObjectStorageClassStandard], 10) + "," +
			strconv.FormatInt(r.responseRecorder.deltaSizeInfo[common.ObjectStorageClassStandardIa], 10) + "," +
			strconv.FormatInt(r.responseRecorder.deltaSizeInfo[common.ObjectStorageClassGlacier], 10)
	}
	return r.emptyValue
}

// Set sets key to value in the r.customReplacements map.
func (r *replacer) Set(key, value string) {
	r.customReplacements["{"+key+"}"] = value
}

type JudgeCdnRequest func(r *http.Request) bool

func judgeCdnRequestFromQuery(r *http.Request) bool {
	cdnFlag, ok := r.URL.Query()["X-Oss-Referer"]
	if ok && len(cdnFlag) > 0 && cdnFlag[0] == "cdn" {
		return true
	}
	return false
}
