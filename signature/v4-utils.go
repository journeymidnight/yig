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

package signature

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	. "github.com/journeymidnight/yig/error"
)

// http Header "x-amz-content-sha256" == "UNSIGNED-PAYLOAD" indicates that the
// client did not calculate sha256 of the payload.
const (
	UnsignedPayload = "UNSIGNED-PAYLOAD"
)

// isValidRegion - verify if incoming region value is valid with configured Region.
func isValidRegion(reqRegion string) bool {
	return true
}

// sumHMAC calculate hmac between two input byte array.
func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

// Reserved string regexp.
var reservedNames = regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")

// getURLEncodedName encode the strings from UTF-8 byte representations to HTML hex escape sequences
//
// This is necessary since regular url.Parse() and url.Encode() functions do not support UTF-8
// non english characters cannot be parsed due to the nature in which url.Encode() is written
//
// This function on the other hand is a direct replacement for url.Encode() technique to support
// pretty much every UTF-8 character.
func getURLEncodedName(name string) string {
	// if object matches reserved string, no need to encode them
	if reservedNames.MatchString(name) {
		return name
	}
	var encodedName string
	for _, s := range name {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedName = encodedName + string(s)
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedName = encodedName + string(s)
			continue
		default:
			len := utf8.RuneLen(s)
			if len < 0 {
				return name
			}
			u := make([]byte, len)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedName = encodedName + "%" + strings.ToUpper(hex)
			}
		}
	}
	return encodedName
}

// getCanonicalHeaders extract signed headers from Authorization header and form the required string:
//
// Lowercase(<HeaderName1>)+":"+Trim(<value>)+"\n"
// Lowercase(<HeaderName2>)+":"+Trim(<value>)+"\n"
// ...
// Lowercase(<HeaderNameN>)+":"+Trim(<value>)+"\n"
//
// Return ErrMissingRequiredSignedHeader if a header is missing in http header but exists in signedHeaders
func getCanonicalHeaders1(signedHeaders []string, req *http.Request) (string, error) {
	reqQueries := req.URL.Query()
	canonicalHeaders := ""
	for _, header := range signedHeaders {
		values, ok := req.Header[http.CanonicalHeaderKey(header)]
		if !ok {
			// try to set headers from Query String
			values, ok = reqQueries[header]
		}

		// Golang http server strips off 'Expect' header, if the
		// client sent this as part of signed headers we need to
		// handle otherwise we would see a signature mismatch.
		// `aws-cli` sets this as part of signed headers.
		//
		// According to
		// http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.20
		// Expect header is always of form:
		//
		//   Expect       =  "Expect" ":" 1#expectation
		//   expectation  =  "100-continue" | expectation-extension
		//
		// So it safe to assume that '100-continue' is what would
		// be sent, for the time being keep this work around.
		// Adding a *TODO* to remove this later when Golang server
		// doesn't filter out the 'Expect' header.
		if header == "expect" {
			values = []string{"100-continue"}
			ok = true
		}
		// Golang http server promotes 'Host' header to Request.Host field
		// and removed from the Header map.
		if header == "host" {
			values = []string{req.Host}
			ok = true
		}
		if !ok {
			return "", ErrMissingRequiredSignedHeader
		}
		canonicalHeaders += header + ":"
		for idx, v := range values {
			if idx > 0 {
				canonicalHeaders += ","
			}
			canonicalHeaders += v
		}
		canonicalHeaders += "\n"
	}
	return canonicalHeaders, nil
}

// extractSignedHeaders extract signed headers from Authorization header
func extractSignedHeaders(signedHeaders []string, r *http.Request) (http.Header, error) {
	reqHeaders := r.Header
	reqQueries := r.URL.Query()
	// find whether "host" is part of list of signed headers.
	// if not return ErrUnsignedHeaders. "host" is mandatory.
	if !contains(signedHeaders, "host") {
		return nil, ErrMissingRequiredSignedHeader
	}
	extractedSignedHeaders := make(http.Header)
	for _, header := range signedHeaders {
		// `host` will not be found in the headers, can be found in r.Host.
		// but its alway necessary that the list of signed headers containing host in it.
		val, ok := reqHeaders[http.CanonicalHeaderKey(header)]
		if !ok {
			// try to set headers from Query String
			val, ok = reqQueries[header]
		}
		if ok {
			for _, enc := range val {
				extractedSignedHeaders.Add(header, enc)
			}
			continue
		}
		switch header {
		case "expect":
			// Golang http server strips off 'Expect' header, if the
			// client sent this as part of signed headers we need to
			// handle otherwise we would see a signature mismatch.
			// `aws-cli` sets this as part of signed headers.
			//
			// According to
			// http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.20
			// Expect header is always of form:
			//
			//   Expect       =  "Expect" ":" 1#expectation
			//   expectation  =  "100-continue" | expectation-extension
			//
			// So it safe to assume that '100-continue' is what would
			// be sent, for the time being keep this work around.
			// Adding a *TODO* to remove this later when Golang server
			// doesn't filter out the 'Expect' header.
			extractedSignedHeaders.Set(header, "100-continue")
		case "host":
			// Go http server removes "host" from Request.Header
			extractedSignedHeaders.Set(header, r.Host)
		case "transfer-encoding":
			// Go http server removes "host" from Request.Header
			for _, enc := range r.TransferEncoding {
				extractedSignedHeaders.Add(header, enc)
			}
		case "content-length":
			// Signature-V4 spec excludes Content-Length from signed headers list for signature calculation.
			// But some clients deviate from this rule. Hence we consider Content-Length for signature
			// calculation to be compatible with such clients.
			extractedSignedHeaders.Set(header, strconv.FormatInt(r.ContentLength, 10))
		default:
			return nil, ErrMissingRequiredSignedHeader
		}
	}
	return extractedSignedHeaders, nil
}

func contains(slice interface{}, elem interface{}) bool {
	v := reflect.ValueOf(slice)
	if v.Kind() == reflect.Slice {
		for i := 0; i < v.Len(); i++ {
			if v.Index(i).Interface() == elem {
				return true
			}
		}
	}
	return false
}

// Trim leading and trailing spaces and replace sequential spaces with one space, following Trimall()
// in http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
func signV4TrimAll(input string) string {
	// Compress adjacent spaces (a space is determined by
	// unicode.IsSpace() internally here) to one space and return
	return strings.Join(strings.Fields(input), " ")
}
