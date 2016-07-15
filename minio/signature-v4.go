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

// This file implements helper functions to validate AWS
// Signature Version '4' authorization header.
//
// This package provides comprehensive helpers for following signature
// types.
// - Based on Authorization header.
// - Based on Query parameters.
// - Based on Form POST policy.
package minio

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sort"
	"strings"
	"time"

	"git.letv.cn/yig/yig/iam"
	. "git.letv.cn/yig/yig/minio/datatype"
)

// AWS Signature Version '4' constants.
const (
	signV4Algorithm = "AWS4-HMAC-SHA256"
)

// getCanonicalHeaders generate a list of request headers with their values
func getCanonicalHeaders(signedHeaders http.Header) string {
	var buf bytes.Buffer
	for k, values := range signedHeaders {
		buf.WriteString(k)
		buf.WriteByte(':')
		for idx, v := range values {
			if idx > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(v)
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

// getSignedHeaders generate a string i.e alphabetically sorted, semicolon-separated list of lowercase request header names
func getSignedHeaders(signedHeaders http.Header) string {
	var headers []string
	for k := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
	}
	headers = append(headers, "host")
	sort.Strings(headers)
	return strings.Join(headers, ";")
}

// getCanonicalRequest generate a canonical request of style
//
// canonicalRequest =
//  <HTTPMethod>\n
//  <CanonicalURI>\n
//  <CanonicalQueryString>\n
//  <CanonicalHeaders>\n
//  <SignedHeaders>\n
//  <HashedPayload>
//
func getCanonicalRequest(extractedSignedHeaders http.Header, payload, queryStr,
	urlPath, method string, signedHeaders []string) string {
	rawQuery := strings.Replace(queryStr, "+", "%20", -1)
	encodedPath := getURLEncodedName(urlPath)
	canonicalRequest := strings.Join([]string{
		method,
		encodedPath,
		rawQuery,
		getCanonicalHeaders(extractedSignedHeaders),
		strings.Join(signedHeaders, ";"),
		payload,
	}, "\n")
	return canonicalRequest
}

// getScope generate a string of a specific date, an AWS region, and a service.
func getScope(t time.Time, region string) string {
	scope := strings.Join([]string{
		t.Format(YYYYMMDD),
		region,
		"s3",
		"aws4_request",
	}, "/")
	return scope
}

// getStringToSign a string based on selected query values.
func getStringToSign(canonicalRequest string, t time.Time, region string) string {
	stringToSign := signV4Algorithm + "\n" + t.Format(Iso8601Format) + "\n"
	stringToSign = stringToSign + getScope(t, region) + "\n"
	canonicalRequestBytes := sha256.Sum256([]byte(canonicalRequest))
	stringToSign = stringToSign + hex.EncodeToString(canonicalRequestBytes[:])
	return stringToSign
}

// getSigningKey hmac seed to calculate final signature.
func getSigningKey(secretKey string, t time.Time, region string) []byte {
	date := sumHMAC([]byte("AWS4"+secretKey), []byte(t.Format(YYYYMMDD)))
	regionBytes := sumHMAC(date, []byte(region))
	service := sumHMAC(regionBytes, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))
	return signingKey
}

// getSignature final signature in hexadecimal form.
func getSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

// doesPolicySignatureMatch - Verify query headers with post policy
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func doesPolicySignatureMatch(formValues map[string]string) APIErrorCode {
	// Parse credential tag.
	credHeader, err := parseCredential(formValues["X-Amz-Credential"])
	if err != ErrNone {
		return ErrMissingFields
	}

	// Verify if the region is valid.
	region := credHeader.scope.region
	if !isValidRegion(region) {
		return ErrInvalidRegion
	}

	// Parse date string.
	t, e := time.Parse(Iso8601Format, formValues["X-Amz-Date"])
	if e != nil {
		return ErrMalformedDate
	}

	secretKey, e := iam.GetSecretKey(credHeader.accessKey)
	if e != nil {
		return ErrInvalidAccessKeyID
	}
	// Get signing key.
	signingKey := getSigningKey(secretKey, t, region)

	// Get signature.
	newSignature := getSignature(signingKey, formValues["Policy"])

	// Verify signature.
	if newSignature != formValues["X-Amz-Signature"] {
		return ErrSignatureDoesNotMatch
	}
	return ErrNone
}

// doesPresignedSignatureMatch - Verify query headers with presigned signature
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func doesPresignedSignatureMatch(r *http.Request, validateRegion bool) APIErrorCode {
	// Parse request query string.
	preSignValues, err := parsePreSignV4(r.URL.Query(), r.Header)
	if err != ErrNone {
		return err
	}

	if preSignValues.Expires > PresignedUrlExpireLimit {
		return ErrMalformedExpires
	}
	if time.Now().Sub(preSignValues.Date) > time.Duration(preSignValues.Expires) {
		return ErrExpiredPresignRequest
	}

	// Verify if region is valid.
	region := preSignValues.Credential.scope.region
	// Should validate region, only if region is set. Some operations
	// do not need region validated for example GetBucketLocation.
	if validateRegion && !isValidRegion(region) {
		return ErrInvalidRegion
	}

	// Extract all the signed headers along with its values.
	extractedSignedHeaders := extractSignedHeaders(preSignValues.SignedHeaders, r)

	/// Verify finally if signature is same.

	// Get canonical request.
	presignedCanonicalReq := getCanonicalRequest(extractedSignedHeaders, unsignedPayload,
		r.URL.Query().Encode(), r.URL.Path, r.Method, preSignValues.SignedHeaders)

	// Get string to sign from canonical request.
	presignedStringToSign := getStringToSign(presignedCanonicalReq, preSignValues.Date, region)

	secretKey, e := iam.GetSecretKey(preSignValues.Credential.accessKey)
	if e != nil {
		return ErrInvalidAccessKeyID
	}
	// Get hmac presigned signing key.
	presignedSigningKey := getSigningKey(secretKey, preSignValues.Date, region)

	// Get new signature.
	newSignature := getSignature(presignedSigningKey, presignedStringToSign)

	// Verify signature.
	if preSignValues.Signature != newSignature {
		return ErrSignatureDoesNotMatch
	}
	return ErrNone
}

// doesSignatureMatch - Verify authorization header with calculated header in accordance with
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func doesSignatureMatch(hashedPayload string, r *http.Request, validateRegion bool) APIErrorCode {
	// Save authorization header.
	v4Auth := r.Header.Get("Authorization")

	// Parse signature version '4' header.
	signV4Values, err := parseSignV4(v4Auth, r.Header)
	if err != ErrNone {
		return err
	}

	// Hashed payload mismatch, return content sha256 mismatch.
	// http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
	// The x-amz-content-sha256 header is required for all AWS Signature Version 4 requests.
	// It provides a hash of the request payload. If there is no payload, you must provide
	// the hash of an empty string.
	if hashedPayload != r.Header.Get("X-Amz-Content-Sha256") {
		return ErrContentSHA256Mismatch
	}

	// Extract all the signed headers along with its values.
	extractedSignedHeaders := extractSignedHeaders(signV4Values.SignedHeaders, r)

	// Verify if region is valid.
	region := signV4Values.Credential.scope.region
	// Should validate region, only if region is set. Some operations
	// do not need region validated for example GetBucketLocation.
	if validateRegion && !isValidRegion(region) {
		return ErrInvalidRegion
	}

	// Extract date, if not present throw error.
	var date string
	if date = r.Header.Get("x-amz-date"); date == "" {
		if date = r.Header.Get("Date"); date == "" {
			return ErrMissingDateHeader
		}
	}
	// Parse date header.
	t, err := ParseAmzDate(date)
	if err != ErrNone {
		return ErrMalformedDate
	}
	diff := time.Now().Sub(t)
	if diff > 15*time.Minute || diff < -15*time.Minute {
		return ErrRequestTimeTooSkewed
	}

	// Query string.
	queryStr := r.URL.Query().Encode()

	// Get canonical request.
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, hashedPayload, queryStr,
		r.URL.Path, r.Method, signV4Values.SignedHeaders)

	// Get string to sign from canonical request.
	stringToSign := getStringToSign(canonicalRequest, t, region)

	secretKey, e := iam.GetSecretKey(signV4Values.Credential.accessKey)
	if e != nil {
		return ErrInvalidAccessKeyID
	}
	// Get hmac signing key.
	signingKey := getSigningKey(secretKey, t, region)

	// Calculate signature.
	newSignature := getSignature(signingKey, stringToSign)

	// Verify if signature match.
	if newSignature != signV4Values.Signature {
		return ErrSignatureDoesNotMatch
	}
	return ErrNone
}
