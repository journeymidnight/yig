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

package signature

import (
	"net/url"
	"strings"
	"time"

	"net/http"
	"sort"

	. "github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/iam"
)

// credentialHeader data type represents structured form of Credential
// string from authorization header.
type credentialHeader struct {
	accessKey string
	scope     struct {
		date    time.Time
		region  string
		service string
		request string
	}
}

func parseCredential(credentialValue string) (credentialHeader, error) {
	credElements := strings.Split(strings.TrimSpace(credentialValue), "/")
	if len(credElements) != 5 {
		return credentialHeader{}, ErrCredMalformed
	}
	if !iam.IsValidAccessKey.MatchString(credElements[0]) {
		return credentialHeader{}, ErrInvalidAccessKeyID
	}
	// Save access key id.
	cred := credentialHeader{
		accessKey: credElements[0],
	}
	var e error
	cred.scope.date, e = time.Parse(YYYYMMDD, credElements[1])
	if e != nil {
		return credentialHeader{}, ErrMalformedDate
	}
	if credElements[2] == "" {
		return credentialHeader{}, ErrInvalidRegion
	}
	cred.scope.region = credElements[2]
	if credElements[3] != "s3" {
		return credentialHeader{}, ErrInvalidService
	}
	cred.scope.service = credElements[3]
	if credElements[4] != "aws4_request" {
		return credentialHeader{}, ErrInvalidRequestVersion
	}
	cred.scope.request = credElements[4]
	return cred, nil
}

// parse credentialHeader string into its structured form.
// Credential=<your-access-key-id>/<date>/<aws-region>/<aws-service>/aws4_request
// <aws-service> is always "s3" for us
func parseCredentialHeader(credElement string) (credentialHeader, error) {
	creds := strings.Split(strings.TrimSpace(credElement), "=")
	if len(creds) != 2 {
		return credentialHeader{}, ErrMissingFields
	}
	if creds[0] != "Credential" {
		return credentialHeader{}, ErrMissingCredTag
	}
	return parseCredential(creds[1])
}

// Parse signature string.
func parseSignature(signElement string) (string, error) {
	signFields := strings.Split(strings.TrimSpace(signElement), "=")
	if len(signFields) != 2 {
		return "", ErrMissingFields
	}
	if signFields[0] != "Signature" {
		return "", ErrMissingSignTag
	}
	signature := signFields[1]
	return signature, nil
}

func headerSigned(header string, headers []string) bool {
	i := sort.SearchStrings(headers, header) // since headers are sorted
	if i < len(headers) && headers[i] == header {
		return true
	}
	return false
}

func parseSignedHeadersContent(signedHeader string, headers http.Header,
	requireContentType bool) ([]string, error) {
	signedHeaders := strings.Split(signedHeader, ";")
	// It's implied in the calculation process that the headers are sorted
	if !sort.StringsAreSorted(signedHeaders) {
		return nil, ErrSignedHeadersNotSorted
	}

	// Check if all required headers are signed, i.e.
	//  Host
	//  Content-Type(required if present in request, not needed in presigned auth)
	//  X-Amz-* headers
	for k, _ := range headers {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "x-amz-") {
			if !headerSigned(lower, signedHeaders) {
				return nil, ErrMissingRequiredSignedHeader
			}
		}
	}
	if !headerSigned("host", signedHeaders) {
		return nil, ErrMissingRequiredSignedHeader
	}
	if requireContentType && headers.Get("content-type") != "" {
		if !headerSigned("content-type", signedHeaders) {
			return nil, ErrMissingRequiredSignedHeader
		}
	}

	return signedHeaders, nil
}

// Parse signed headers string.
// SignedHeaders is a semicolon-separated list of request headers names used to
// compute signature, must be in lowercase. e.g. host;range;x-amz-date
func parseSignedHeaders(signedHdrElement string, headers http.Header,
	requireContentType bool) ([]string, error) {
	signedHdrFields := strings.Split(strings.TrimSpace(signedHdrElement), "=")
	if len(signedHdrFields) != 2 {
		return nil, ErrMissingFields
	}
	if signedHdrFields[0] != "SignedHeaders" {
		return nil, ErrMissingSignHeadersTag
	}
	return parseSignedHeadersContent(signedHdrFields[1], headers, requireContentType)
}

// signValues data type represents structured form of AWS Signature V4 header.
type signValues struct {
	Credential    credentialHeader
	SignedHeaders []string
	Signature     string
}

// preSignValues data type represents structued form of AWS Signature V4 query string.
type preSignValues struct {
	signValues
	Date    time.Time
	Expires time.Duration
}

// Parses signature version '4' query string of the following form.
//
//   querystring = X-Amz-Algorithm=algorithm
//   querystring += &X-Amz-Credential= urlencode(accessKey + '/' + credential_scope)
//   querystring += &X-Amz-Date=date
//   querystring += &X-Amz-Expires=timeout interval
//   querystring += &X-Amz-SignedHeaders=signed_headers
//   querystring += &X-Amz-Signature=signature
//
func parsePreSignV4(query url.Values, headers http.Header) (preSignValues, error) {
	// Verify if the query algorithm is supported or not.
	if query.Get("X-Amz-Algorithm") != signV4Algorithm {
		return preSignValues{}, ErrInvalidQuerySignatureAlgo
	}

	// Initialize signature version '4' structured header.
	preSignV4Values := preSignValues{}

	var err error
	// Save credential.
	preSignV4Values.Credential, err = parseCredential(query.Get("X-Amz-Credential"))
	if err != nil {
		return preSignValues{}, err
	}

	// Save date in native time.Time.
	preSignV4Values.Date, err = time.Parse(Iso8601Format, query.Get("X-Amz-Date"))
	if err != nil {
		return preSignValues{}, ErrMalformedDate
	}

	// Save expires in native time.Duration.
	preSignV4Values.Expires, err = time.ParseDuration(query.Get("X-Amz-Expires") + "s")
	if err != nil {
		return preSignValues{}, ErrMalformedExpires
	}

	// Save signed headers.
	preSignV4Values.SignedHeaders, err =
		parseSignedHeadersContent(query.Get("X-Amz-SignedHeaders"), headers, false)
	if err != nil {
		return preSignValues{}, err
	}

	// Save signature.
	preSignV4Values.Signature = query.Get("X-Amz-Signature")

	// Return structured form of signature query string.
	return preSignV4Values, nil
}

// Parses signature version '4' header of the following form.
//
//    Authorization: algorithm Credential=XXX,SignedHeaders=XXX,Signature=XXX
//
func parseSignV4(v4Auth string, headers http.Header) (signValues, error) {
	// Replace all spaced strings, some clients can send spaced
	// parameters and some won't. So we pro-actively remove any spaces
	// to make parsing easier.
	v4Auth = strings.Replace(v4Auth, " ", "", -1)
	if v4Auth == "" {
		return signValues{}, ErrAuthHeaderEmpty
	}

	// Strip off the Algorithm prefix.
	v4Auth = strings.TrimPrefix(v4Auth, signV4Algorithm)
	authFields := strings.Split(strings.TrimSpace(v4Auth), ",")
	if len(authFields) != 3 {
		return signValues{}, ErrMissingFields
	}

	// Initialize signature version '4' structured header.
	signV4Values := signValues{}

	var err error
	// Save credential values.
	signV4Values.Credential, err = parseCredentialHeader(authFields[0])
	if err != nil {
		return signValues{}, err
	}

	// Save signed headers.
	// Usually we should have content-type in SignedHeaders, But in offical Amazon's S3
	// PHP SDK, it does not sign content-type since 3.*. So we do not verify content-type
	// in SignedHeaders
	signV4Values.SignedHeaders, err = parseSignedHeaders(authFields[1], headers, false)
	if err != nil {
		return signValues{}, err
	}

	// Save signature.
	signV4Values.Signature, err = parseSignature(authFields[2])
	if err != nil {
		return signValues{}, err
	}

	// Return the structure here.
	return signV4Values, nil
}
