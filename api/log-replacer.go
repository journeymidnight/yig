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
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/signature"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
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

		// append unescaped prefix + replacement
		result += strings.TrimPrefix(unescapeBraces(s[:idxStart]), "\\") + replacement

		// strip out scanned parts
		s = s[idxEnd+1:]
	}

	// append unscanned parts
	return result + unescapeBraces(s)
}

// getSubstitution retrieves value from corresponding key
func (r *replacer) getSubstitution(key string) string {

	// search default replacements in the end
	switch key {
	case "{time_local}":
		timeLocal := time.Now().Format("2006-01-02 15:04:05")
		return "[" + timeLocal + "]"
	case "{request}":
		request := r.request.Method + " " + r.request.URL.String() + " " + r.request.Proto
		return request
	case "{host}":
		return r.request.Host
	case "{remote_addr}":
		return r.request.RemoteAddr
	case "{http_x_real_ip}":
		return r.request.Header.Get("X-Real-Ip")
	case "{request_length}":
		requestLength := r.request.ContentLength
		result := strconv.FormatInt(requestLength, 10)
		return result
	case "{http_user_agent}":
		return "\"" + r.request.Header.Get("User-Agent") + "\""
	case "{retain}":
		return "-"
	case "{http_referer}":
		forwarded := r.request.Header.Get("X-Forwarded-For")
		http_referers := strings.SplitN(forwarded, ",", -1)
		http_referer := http_referers[0]
		return http_referer
	case "{bucket_name}":
		bucketName := "-"
		s3Endpoint := helper.CONFIG.S3Domain[0]
		splits := strings.SplitN(r.request.URL.Path[1:], "/", 2)
		v := strings.Split(r.request.Host, ":")
		hostWithOutPort := v[0]
		if strings.HasSuffix(hostWithOutPort, "."+s3Endpoint) {
			bucketName = strings.TrimSuffix(hostWithOutPort, "."+s3Endpoint)
		} else {
			if len(splits) == 1 {
				bucketName = splits[0]
			}
			if len(splits) == 2 {
				bucketName = splits[0]
			}
		}
		return bucketName
	case "{project_id}":
		projectId := "-"
		credential, err := signature.IsReqAuthenticated(r.request)
		if err == nil {
			projectId = credential.UserId
		}
		return projectId
	case "{body_bytes_sent}":
		bodyBytesSent := r.responseRecorder.size
		result := strconv.Itoa(bodyBytesSent)
		return result
	case "{status}":
		status := r.responseRecorder.status
		result := strconv.Itoa(status)
		return result
	case "{request_time}":
		requestTime := r.responseRecorder.requestTime
		//Duration convert to Millisecond
		temp := requestTime.Nanoseconds() / 1e6
		result := strconv.FormatInt(temp, 10)
		return result
	case "{is_private_subnet}":
		remoteAddr := r.request.RemoteAddr
		remoteAddrIP := strings.SplitN(remoteAddr, ":", -1)[0]
		isPrivateSubnet := isPrivateSubnet(net.ParseIP(remoteAddrIP))
		result := strconv.FormatBool(isPrivateSubnet)
		return result
	case "{region_id}":
		regionId := helper.CONFIG.Region
		return regionId
	default:
		// {labelN}
		if strings.HasPrefix(key, "{label") {
			nStr := key[6 : len(key)-1] // get the integer N in "{labelN}"
			n, err := strconv.Atoi(nStr)
			if err != nil || n < 1 {
				return r.emptyValue
			}
			labels := strings.Split(r.request.Host, ".")
			if n > len(labels) {
				return r.emptyValue
			}
			return labels[n-1]
		}
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

const (
	CombinedLogFormat = "{time_local} {request} {host} {bucket_name} {remote_addr} {http_x_real_ip} {project_id} {request_length} " +
		"{retain} {retain} {request_time} {status} {body_bytes_sent} {http_referer} {http_user_agent} {is_private_subnet} {region_id}"
)
