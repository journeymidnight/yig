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
package signature

import (
	"bytes"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"net/textproto"
	"sort"
	"strings"
	"time"

	. "github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/brand"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/sts"
)

// getSignedHeaders generate a string i.e alphabetically sorted,
// semicolon-separated list of lowercase request header names
func getSignedHeaders(signedHeaders http.Header) string {
	var headers []string
	for k := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
	}
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
	urlPath, method string) string {
	rawQuery := strings.Replace(queryStr, "+", "%20", -1)
	encodedPath := getURLEncodedName(urlPath)
	canonicalRequest := strings.Join([]string{
		method,
		encodedPath,
		rawQuery,
		getCanonicalHeaders(extractedSignedHeaders),
		getSignedHeaders(extractedSignedHeaders),
		payload,
	}, "\n")
	return canonicalRequest
}

// getCanonicalHeaders generate a list of request headers with their values
func getCanonicalHeaders(signedHeaders http.Header) string {
	var headers []string
	vals := make(http.Header)
	for k, vv := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
		vals[strings.ToLower(k)] = vv
	}
	sort.Strings(headers)

	var buf bytes.Buffer
	for _, k := range headers {
		buf.WriteString(k)
		buf.WriteByte(':')
		for idx, v := range vals[k] {
			if idx > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(signV4TrimAll(v))
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

// getScope generate a string of a specific date, an AWS region, and a service.
func getScope(t time.Time, region string, brandName Brand) string {
	scope := strings.Join([]string{
		t.Format(YYYYMMDD),
		region,
		"s3",
		strings.ToLower(brandName.GetSpecialFieldFullName(SignRequest)),
	}, "/")
	return scope
}

// getStringToSign a string based on selected query values.
func getStringToSign(canonicalRequest string, brandName Brand, t time.Time, region string) string {
	stringToSign := brandName.GetSpecialFieldFullName(SignV4Algorithm) + "\n" + t.Format(Iso8601Format) + "\n"
	stringToSign = stringToSign + getScope(t, region, brandName) + "\n"
	canonicalRequestBytes := sum256([]byte(canonicalRequest))
	stringToSign = stringToSign + hex.EncodeToString(canonicalRequestBytes[:])
	return stringToSign
}

// getSigningKey hmac seed to calculate final signature.
func getSigningKey(secretKey string, brandName Brand, t time.Time, region string) []byte {
	date := sumHMAC([]byte(brandName.GetSpecialFieldFullName(SignV4)+secretKey), []byte(t.Format(YYYYMMDD)))
	regionBytes := sumHMAC(date, []byte(region))
	service := sumHMAC(regionBytes, []byte("s3"))
	signingKey := sumHMAC(service, []byte(strings.ToLower(brandName.GetSpecialFieldFullName(SignRequest))))
	return signingKey
}

// getSignature final signature in hexadecimal form.
func getSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

// doesPolicySignatureMatch - Verify query headers with post policy
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func DoesPolicySignatureMatchV4(formValues map[string]string, brandName Brand) (credential common.Credential, err error) {
	// Parse credential tag.
	credHeader, err := parseCredential(formValues[brandName.GetGeneralFieldFullName(XCredential)], brandName)
	if err != nil {
		return credential, err
	}

	// Verify if the region is valid.
	region := credHeader.scope.region
	if !isValidRegion(region) {
		return credential, ErrInvalidRegion
	}

	// Parse date string.
	t, e := time.Parse(Iso8601Format, formValues[brandName.GetGeneralFieldFullName(XDate)])
	if e != nil {
		return credential, ErrMalformedDate
	}

	credential, e = iam.GetCredential(credHeader.accessKey)
	if e != nil {
		return credential, e
	}
	// Get signing key.
	signingKey := getSigningKey(credential.SecretAccessKey, brandName, t, region)

	// Get signature.
	newSignature := getSignature(signingKey, formValues["Policy"])

	// Verify signature.
	if newSignature != formValues[brandName.GetGeneralFieldFullName(XSignature)] {
		return credential, ErrSignatureDoesNotMatch
	}
	return credential, nil
}

// doesPresignedSignatureMatch - Verify query headers with presigned signature
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func DoesPresignedSignatureMatchV4(r *http.Request, brandName Brand,
	validateRegion bool) (credential common.Credential, err error) {
	// Parse request query string.
	preSignValues, err := parsePreSignV4(r.URL.Query(), r.Header, brandName)
	if err != nil {
		return credential, err
	}

	if securityToken := r.URL.Query().Get(brandName.GetGeneralFieldFullName(XSecurityToken)); securityToken != "" {
		credential, err = sts.VerifyToken(preSignValues.Credential.accessKey,
			securityToken)
	} else {
		credential, err = iam.GetCredential(preSignValues.Credential.accessKey)
	}
	if err != nil {
		return common.Credential{}, err
	}

	if preSignValues.Expires > PresignedUrlExpireLimit {
		return credential, ErrMalformedExpires
	}
	if time.Now().Sub(preSignValues.Date) > time.Duration(preSignValues.Expires) {
		return credential, ErrExpiredPresignRequest
	}

	// Verify if region is valid.
	region := preSignValues.Credential.scope.region
	// Should validate region, only if region is set. Some operations
	// do not need region validated for example GetBucketLocation.
	if validateRegion && !isValidRegion(region) {
		return credential, ErrInvalidRegion
	}

	// Extract all the signed headers along with its values.
	extractedSignedHeaders, err := extractSignedHeaders(preSignValues.SignedHeaders, r)
	if err != nil {
		return
	}

	/// Verify finally if signature is same.

	// Construct the query.
	query := r.URL.Query()
	query.Del(brandName.GetGeneralFieldFullName(XSignature))

	// FIXME: Due to some business reasons, some non-S3 headers will not be signed
	for k := range query {
		lk := textproto.CanonicalMIMEHeaderKey(k)
		if strings.HasPrefix(lk, "X-") && !strings.HasPrefix(lk, brandName.GetGeneralFieldFullName(XGeneralName)) {
			query.Del(k)
		}
	}

	presignedCanonicalReq := getCanonicalRequest(extractedSignedHeaders, UnsignedPayload,
		query.Encode(), r.URL.Path, r.Method)

	// Get string to sign from canonical request.
	presignedStringToSign := getStringToSign(presignedCanonicalReq, brandName, preSignValues.Date, region)

	// Get hmac presigned signing key.
	presignedSigningKey := getSigningKey(credential.SecretAccessKey, brandName, preSignValues.Date, region)

	// Get new signature.
	newSignature := getSignature(presignedSigningKey, presignedStringToSign)

	// Verify signature.
	if preSignValues.Signature != newSignature {
		return credential, ErrSignatureDoesNotMatch
	}
	return credential, nil
}

// get credential but not verify it, used only for signed v4 auth
func getCredentialUnverified(r *http.Request, brandName Brand) (credential common.Credential, err error) {
	v4Auth := r.Header.Get("Authorization")

	signV4Values, err := parseSignV4(v4Auth, r.Header, brandName)
	if err != nil {
		return credential, err
	}

	if securityToken := r.Header.Get(brandName.GetGeneralFieldFullName(XSecurityToken)); securityToken != "" {
		credential, err = sts.VerifyToken(signV4Values.Credential.accessKey,
			securityToken)
	} else {
		credential, err = iam.GetCredential(signV4Values.Credential.accessKey)
	}
	return credential, err
}

// doesSignatureMatch - Verify authorization header with calculated header in accordance with
//     - http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func DoesSignatureMatchV4(hashedPayload string, r *http.Request, brandName Brand,
	validateRegion bool) (credential common.Credential, err error) {
	// Save authorization header.
	v4Auth := r.Header.Get("Authorization")

	// Parse signature version '4' header.
	signV4Values, err := parseSignV4(v4Auth, r.Header, brandName)
	if err != nil {
		return credential, err
	}

	// Hashed payload mismatch, return content sha256 mismatch.
	// http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
	// The x-amz-content-sha256 header is required for all AWS Signature Version 4 requests.
	// It provides a hash of the request payload. If there is no payload, you must provide
	// the hash of an empty string.
	hashedPayloadReceived := r.Header.Get(brandName.GetGeneralFieldFullName(XContentSha))
	if hashedPayloadReceived != "UNSIGNED-PAYLOAD" && hashedPayloadReceived != hashedPayload {
		return credential, ErrContentSHA256Mismatch
	}

	// Extract all the signed headers along with its values.
	extractedSignedHeaders, err := extractSignedHeaders(signV4Values.SignedHeaders, r)
	if err != nil {
		return credential, err
	}

	// Verify if region is valid.
	region := signV4Values.Credential.scope.region
	// Should validate region, only if region is set. Some operations
	// do not need region validated for example GetBucketLocation.
	if validateRegion && !isValidRegion(region) {
		return credential, ErrInvalidRegion
	}

	// Extract date, if not present throw error.
	var date string
	if date = r.Header.Get(brandName.GetGeneralFieldFullName(XDate)); date == "" {
		if date = r.Header.Get("Date"); date == "" {
			return credential, ErrMissingDateHeader
		}
	}
	// Parse date header.
	t, err := ParseAmzDate(date)
	if err != nil {
		return credential, err
	}
	diff := time.Now().Sub(t)
	if diff > 15*time.Minute || diff < -15*time.Minute {
		return credential, ErrRequestTimeTooSkewed
	}

	// Query string.
	queryStr := r.URL.Query().Encode()

	// Get canonical request.
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, hashedPayloadReceived, queryStr,
		r.URL.Path, r.Method)

	// Get string to sign from canonical request.
	stringToSign := getStringToSign(canonicalRequest, brandName, t, region)

	if securityToken := r.Header.Get(brandName.GetGeneralFieldFullName(XSecurityToken)); securityToken != "" {
		credential, err = sts.VerifyToken(signV4Values.Credential.accessKey,
			securityToken)
	} else {
		credential, err = iam.GetCredential(signV4Values.Credential.accessKey)
	}
	if err != nil {
		return credential, err
	}
	// Get hmac signing key.
	signingKey := getSigningKey(credential.SecretAccessKey, brandName, t, region)

	// Calculate signature.
	newSignature := getSignature(signingKey, stringToSign)

	// Verify if signature match.
	if newSignature != signV4Values.Signature {
		return credential, ErrSignatureDoesNotMatch
	}
	return credential, nil
}

// compareSignatureV4 returns true if and only if both signatures
// are equal. The signatures are expected to be HEX encoded strings
// according to the AWS S3 signature V4 spec.
func compareSignatureV4(sig1, sig2 string) bool {
	// The CTC using []byte(str) works because the hex encoding
	// is unique for a sequence of bytes. See also compareSignatureV2.
	return subtle.ConstantTimeCompare([]byte(sig1), []byte(sig2)) == 1
}
