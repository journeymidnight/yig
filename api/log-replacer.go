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
	"bytes"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/signature"
)

const (
	CombinedLogFormat = "{time_local} {request_uri} {request_id} {operation} {host_name} {bucket_name} {object_name} " +
		"{object_size} {requester_id} {project_id} {remote_addr} {http_x_real_ip} {request_length} {server_cost} " +
		"{request_time} {http_status} {error_code} {body_bytes_sent} {http_referer} {http_user_agent}"

	BillingLogFormat = "{is_internal} {storage_class} {target_storage_class} {bucket_logging} {cdn_request}"
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
		return r.request.Context().Value(RequestContextKey).(RequestContext).RequestId
	case "{operation_name}":
		return r.responseRecorder.operationName
	case "{host_name}":
		return r.request.Host
	case "{region_id}":
		return helper.CONFIG.Region
	case "{bucket_name}":
		bucketName, _ := GetBucketAndObjectInfoFromRequest(r.request)
		if bucketName == "" {
			return "-"
		}
		return bucketName
	case "{object_name}":
		_, objectName := GetBucketAndObjectInfoFromRequest(r.request)
		if objectName == "" {
			return "-"
		}
		return objectName
	case "{object_size}":
		var objectSize int64
		objectInfo := r.request.Context().Value(RequestContextKey).(RequestContext).ObjectInfo
		if objectInfo != nil {
			objectSize = objectInfo.Size
		}
		return strconv.FormatInt(objectSize, 10)
	case "{requester_id}":
		requester_id := "-"
		credential, err := signature.IsReqAuthenticated(r.request)
		if err == nil {
			requester_id = credential.UserId
		}
		return requester_id
	case "{project_id}":
		bucketInfo := r.request.Context().Value(RequestContextKey).(RequestContext).BucketInfo
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
	case "{is_private_subnet}":
		// Currently, the intranet domain name is formed by adding the "-internal" on the second-level domain name of the public network.
		return strconv.FormatBool(strings.Contains(r.request.Host, "internal"))
	case "{storage_class}":
		objectInfo := r.request.Context().Value(RequestContextKey).(RequestContext).ObjectInfo
		if objectInfo == nil {
			return "-"
		}
		return objectInfo.StorageClass.ToString()
	case "{target_storage_class}":
		if r.request.Header.Get("X-Amz-Copy-Source") != "" && r.request.Header.Get("X-Amz-Metadata-Directive") != "" {
			storageClassFromHeader, err := getStorageClassFromHeader(r.request)
			if err != nil {
				return "-"
			}
			return storageClassFromHeader.ToString()
		}
		return "-"

	case "{bucket_logging}":
		// TODO: Add bucket logging
		return strconv.FormatBool(false)
	case "{cdn_request}":
		// If you have another judgment method, implement and replace the judgeFunc
		var judgeFunc JudgeCdnRequest
		judgeFunc = judgeCdnRequestFromQuery
		return strconv.FormatBool(judgeFunc(r.request))
	default:
		return "-"
	}
	return r.emptyValue
}

// Set sets key to value in the r.customReplacements map.
func (r *replacer) Set(key, value string) {
	r.customReplacements["{"+key+"}"] = value
}

// isPrivateSubnet - check to see if this ip is in a private subnet
func isPrivateSubnet(ipAddress net.IP) bool {
	// my use case is only concerned with ipv4 atm
	if ipCheck := ipAddress.To4(); ipCheck != nil {
		// iterate over all our ranges
		for _, r := range privateRanges {
			// check if this ip is in a private range
			if inRange(r, ipAddress) {
				return true
			}
		}
	}
	return false
}

//ipRange - a structure that holds the start and end of a range of ip addresses
type ipRange struct {
	start net.IP
	end   net.IP
}

var privateRanges = []ipRange{
	ipRange{
		start: net.ParseIP("10.0.0.0"),
		end:   net.ParseIP("10.255.255.255"),
	},
	ipRange{
		start: net.ParseIP("100.64.0.0"),
		end:   net.ParseIP("100.127.255.255"),
	},
	ipRange{
		start: net.ParseIP("127.0.0.0"),
		end:   net.ParseIP("127.0.0.255"),
	},
	ipRange{
		start: net.ParseIP("172.16.0.0"),
		end:   net.ParseIP("172.31.255.255"),
	},
	ipRange{
		start: net.ParseIP("192.0.0.0"),
		end:   net.ParseIP("192.0.0.255"),
	},
	ipRange{
		start: net.ParseIP("192.168.0.0"),
		end:   net.ParseIP("192.168.255.255"),
	},
	ipRange{
		start: net.ParseIP("198.18.0.0"),
		end:   net.ParseIP("198.19.255.255"),
	},
}

// inRange - check to see if a given ip address is within a range given
func inRange(r ipRange, ipAddress net.IP) bool {
	// strcmp type byte comparison
	if bytes.Compare(ipAddress, r.start) >= 0 && bytes.Compare(ipAddress, r.end) < 0 {
		return true
	}
	return false
}

type JudgeCdnRequest func(r *http.Request) bool

func judgeCdnRequestFromQuery(r *http.Request) bool {
	cdnFlag := r.URL.Query()["X-Amz-Referer"][0]
	if cdnFlag == "cdn" {
		return true
	}
	return false
}
