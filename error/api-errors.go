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

package error

import (
	"net/http"
)

type ApiError interface{
	error
	AwsErrorCode() string
	Description()  string
	HttpStatusCode() int
}

type ApiErrorStruct struct  {
	AwsErrorCode string
	Description string
	HttpStatusCode int
}

// APIErrorCode type of error status.
type ApiErrorCode int

// Error codes, non exhaustive list - http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
const (
	ErrAccessDenied ApiErrorCode = iota
	ErrBadDigest
	ErrBucketAlreadyExists
	ErrEntityTooSmall
	ErrEntityTooLarge
	ErrIncompleteBody
	ErrInternalError
	ErrInvalidAccessKeyID
	ErrInvalidBucketName
	ErrInvalidDigest
	ErrInvalidRange
	ErrInvalidMaxKeys
	ErrInvalidMaxUploads
	ErrInvalidMaxParts
	ErrInvalidPartNumberMarker
	ErrInvalidRequestBody
	ErrInvalidCopySource
	ErrInvalidCopyDest
	ErrInvalidPolicyDocument
	ErrMalformedXML
	ErrMissingContentLength
	ErrMissingContentMD5
	ErrMissingRequestBodyError
	ErrNoSuchBucket
	ErrNoSuchBucketPolicy
	ErrNoSuchKey
	ErrNoSuchUpload
	ErrNotImplemented
	ErrPreconditionFailed
	ErrRequestTimeTooSkewed
	ErrSignatureDoesNotMatch
	ErrMethodNotAllowed
	ErrInvalidPart
	ErrInvalidPartOrder
	ErrAuthorizationHeaderMalformed
	ErrMalformedPOSTRequest
	ErrSignatureVersionNotSupported
	ErrBucketNotEmpty
	ErrBucketAccessForbidden
	ErrMalformedPolicy
	ErrMissingFields
	ErrMissingCredTag
	ErrCredMalformed
	ErrInvalidRegion
	ErrInvalidService
	ErrInvalidRequestVersion
	ErrMissingSignTag
	ErrMissingSignHeadersTag
	ErrMissingRequiredSignedHeader
	ErrSignedHeadersNotSorted
	ErrPolicyAlreadyExpired
	ErrPolicyViolation
	ErrMalformedDate
	ErrMalformedExpires
	ErrAuthHeaderEmpty
	ErrExpiredPresignRequest
	ErrMissingDateHeader
	ErrInvalidQuerySignatureAlgo
	ErrInvalidQueryParams
	ErrBucketAlreadyOwnedByYou
	// Add new error codes here.

	// S3 extended errors.
	ErrContentSHA256Mismatch
	// Add new extended error codes here.

	// Minio extended errors.
	ErrReadQuorum
	ErrWriteQuorum
	ErrStorageFull
	ErrObjectExistsAsDirectory
	ErrPolicyNesting
	ErrInvalidObjectName
	// Add new extended error codes here.
	// Please open a https://github.com/minio/minio/issues before adding
	// new error codes here.
)

// error code to APIError structure, these fields carry respective
// descriptions for all the error responses.
var ErrorCodeResponse = map[ApiErrorCode]ApiErrorStruct{
	ErrInvalidCopyDest: {
		AwsErrorCode:           "InvalidRequest",
		Description:    "This copy request is illegal because it is trying to copy an object to itself.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidCopySource: {
		AwsErrorCode:           "InvalidArgument",
		Description:    "Copy Source must mention the source bucket and key: sourcebucket/sourcekey.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRequestBody: {
		AwsErrorCode:           "InvalidArgument",
		Description:    "Body shouldn't be set for this request.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxUploads: {
		AwsErrorCode:           "InvalidArgument",
		Description:    "Argument max-uploads must be an integer between 0 and 2147483647",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxKeys: {
		AwsErrorCode:           "InvalidArgument",
		Description:    "Argument maxKeys must be an integer between 0 and 2147483647",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxParts: {
		AwsErrorCode:           "InvalidArgument",
		Description:    "Argument max-parts must be an integer between 0 and 2147483647",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidPartNumberMarker: {
		AwsErrorCode:           "InvalidArgument",
		Description:    "Argument partNumberMarker must be an integer.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidPolicyDocument: {
		AwsErrorCode:           "InvalidPolicyDocument",
		Description:    "The content of the form does not meet the conditions specified in the policy document.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrAccessDenied: {
		AwsErrorCode:           "AccessDenied",
		Description:    "Access Denied.",
		HttpStatusCode: http.StatusForbidden,
	},
	ErrBadDigest: {
		AwsErrorCode:           "BadDigest",
		Description:    "The Content-Md5 you specified did not match what we received.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrBucketAlreadyExists: {
		AwsErrorCode:           "BucketAlreadyExists",
		Description:    "The requested bucket name is not available.",
		HttpStatusCode: http.StatusConflict,
	},
	ErrEntityTooSmall: {
		AwsErrorCode:           "EntityTooSmall",
		Description:    "Your proposed upload is smaller than the minimum allowed object size.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrEntityTooLarge: {
		AwsErrorCode:           "EntityTooLarge",
		Description:    "Your proposed upload exceeds the maximum allowed object size.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrIncompleteBody: {
		AwsErrorCode:           "IncompleteBody",
		Description:    "You did not provide the number of bytes specified by the Content-Length HTTP header.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInternalError: {
		AwsErrorCode:           "InternalError",
		Description:    "We encountered an internal error, please try again.",
		HttpStatusCode: http.StatusInternalServerError,
	},
	ErrInvalidAccessKeyID: {
		AwsErrorCode:           "InvalidAccessKeyID",
		Description:    "The access key ID you provided does not exist in our records.",
		HttpStatusCode: http.StatusForbidden,
	},
	ErrInvalidBucketName: {
		AwsErrorCode:           "InvalidBucketName",
		Description:    "The specified bucket is not valid.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidDigest: {
		AwsErrorCode:           "InvalidDigest",
		Description:    "The Content-Md5 you specified is not valid.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRange: {
		AwsErrorCode:           "InvalidRange",
		Description:    "The requested range is not satisfiable",
		HttpStatusCode: http.StatusRequestedRangeNotSatisfiable,
	},
	ErrMalformedXML: {
		AwsErrorCode:           "MalformedXML",
		Description:    "The XML you provided was not well-formed or did not validate against our published schema.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrMissingContentLength: {
		AwsErrorCode:           "MissingContentLength",
		Description:    "You must provide the Content-Length HTTP header.",
		HttpStatusCode: http.StatusLengthRequired,
	},
	ErrMissingContentMD5: {
		AwsErrorCode:           "MissingContentMD5",
		Description:    "Missing required header for this request: Content-Md5.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrMissingRequestBodyError: {
		AwsErrorCode:           "MissingRequestBodyError",
		Description:    "Request body is empty.",
		HttpStatusCode: http.StatusLengthRequired,
	},
	ErrNoSuchBucket: {
		AwsErrorCode:           "NoSuchBucket",
		Description:    "The specified bucket does not exist",
		HttpStatusCode: http.StatusNotFound,
	},
	ErrNoSuchBucketPolicy: {
		AwsErrorCode:           "NoSuchBucketPolicy",
		Description:    "The specified bucket does not have a bucket policy.",
		HttpStatusCode: http.StatusNotFound,
	},
	ErrNoSuchKey: {
		AwsErrorCode:           "NoSuchKey",
		Description:    "The specified key does not exist.",
		HttpStatusCode: http.StatusNotFound,
	},
	ErrNoSuchUpload: {
		AwsErrorCode:           "NoSuchUpload",
		Description:    "The specified multipart upload does not exist.",
		HttpStatusCode: http.StatusNotFound,
	},
	ErrNotImplemented: {
		AwsErrorCode:           "NotImplemented",
		Description:    "A header you provided implies functionality that is not implemented",
		HttpStatusCode: http.StatusNotImplemented,
	},
	ErrPreconditionFailed: {
		AwsErrorCode:           "PreconditionFailed",
		Description:    "At least one of the pre-conditions you specified did not hold",
		HttpStatusCode: http.StatusPreconditionFailed,
	},
	ErrRequestTimeTooSkewed: {
		AwsErrorCode:           "RequestTimeTooSkewed",
		Description:    "The difference between the request time and the server's time is too large.",
		HttpStatusCode: http.StatusForbidden,
	},
	ErrSignatureDoesNotMatch: {
		AwsErrorCode:           "SignatureDoesNotMatch",
		Description:    "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
		HttpStatusCode: http.StatusForbidden,
	},
	ErrMethodNotAllowed: {
		AwsErrorCode:           "MethodNotAllowed",
		Description:    "The specified method is not allowed against this resource.",
		HttpStatusCode: http.StatusMethodNotAllowed,
	},
	ErrInvalidPart: {
		AwsErrorCode:           "InvalidPart",
		Description:    "One or more of the specified parts could not be found.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidPartOrder: {
		AwsErrorCode:           "InvalidPartOrder",
		Description:    "The list of parts was not in ascending order. The parts list must be specified in order by part number.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrAuthorizationHeaderMalformed: {
		AwsErrorCode:           "AuthorizationHeaderMalformed",
		Description:    "The authorization header is malformed.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrMalformedPOSTRequest: {
		AwsErrorCode:           "MalformedPOSTRequest",
		Description:    "The body of your POST request is not well-formed multipart/form-data.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrSignatureVersionNotSupported: {
		AwsErrorCode:           "InvalidRequest",
		Description:    "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrBucketNotEmpty: {
		AwsErrorCode:           "BucketNotEmpty",
		Description:    "The bucket you tried to delete is not empty.",
		HttpStatusCode: http.StatusConflict,
	},
	ErrBucketAccessForbidden: {
		AwsErrorCode:           "BucketAccessForbidden",
		Description:    "You have no access to this bucket.",
		HttpStatusCode: http.StatusForbidden,
	},
	ErrMalformedPolicy: {
		AwsErrorCode:           "MalformedPolicy",
		Description:    "Policy has invalid resource.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrMissingFields: {
		AwsErrorCode:           "MissingFields",
		Description:    "Missing fields in request.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrMissingCredTag: {
		AwsErrorCode:           "InvalidRequest",
		Description:    "Missing Credential field for this request.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrCredMalformed: {
		AwsErrorCode:           "CredentialMalformed",
		Description:    "Credential field malformed does not follow accessKeyID/credScope.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrMalformedDate: {
		AwsErrorCode:           "MalformedDate",
		Description:    "Invalid date format header, expected to be in ISO8601, RFC1123 or RFC1123Z time format.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRegion: {
		AwsErrorCode:           "InvalidRegion",
		Description:    "Region does not match.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidService: {
		AwsErrorCode:           "AccessDenied",
		Description:    "Service scope should be of value 's3'.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRequestVersion: {
		AwsErrorCode:           "AccessDenied",
		Description:    "Request scope should be of value 'aws4_request'.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrMissingSignTag: {
		AwsErrorCode:           "AccessDenied",
		Description:    "Signature header missing Signature field.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrMissingSignHeadersTag: {
		AwsErrorCode:           "InvalidArgument",
		Description:    "Signature header missing SignedHeaders field.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrMissingRequiredSignedHeader: {
		AwsErrorCode:           "InvalidArgument",
		Description:    "Missing one or more required signed header",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrSignedHeadersNotSorted: {
		AwsErrorCode:           "InvalidArgument",
		Description:    "Signed headers are not ordered",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrPolicyAlreadyExpired: {
		AwsErrorCode:           "AccessDenied",
		Description:    "Invalid according to Policy: Policy expired.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrPolicyViolation: {
		AwsErrorCode:           "AccessDenied",
		Description:    "File uploading policy violatedd.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrMalformedExpires: {
		AwsErrorCode:           "MalformedExpires",
		Description:    "Malformed expires value, should be between 1 and 604800(seven days)",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrAuthHeaderEmpty: {
		AwsErrorCode:           "InvalidArgument",
		Description:    "Authorization header is invalid -- one and only one ' ' (space) required.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrMissingDateHeader: {
		AwsErrorCode:           "AccessDenied",
		Description:    "AWS authentication requires a valid Date or x-amz-date header",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidQuerySignatureAlgo: {
		AwsErrorCode:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Algorithm only supports \"AWS4-HMAC-SHA256\".",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrExpiredPresignRequest: {
		AwsErrorCode:           "AccessDenied",
		Description:    "Request has expired.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrInvalidQueryParams: {
		AwsErrorCode:           "AuthorizationQueryParametersError",
		Description:    "Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.",
		HttpStatusCode: http.StatusBadRequest,
	},
	ErrBucketAlreadyOwnedByYou: {
		AwsErrorCode:           "BucketAlreadyOwnedByYou",
		Description:    "Your previous request to create the named bucket succeeded and you already own it.",
		HttpStatusCode: http.StatusConflict,
	},

	/// S3 extensions.
	ErrContentSHA256Mismatch: {
		AwsErrorCode:           "XAmzContentSHA256Mismatch",
		Description:    "The provided 'x-amz-content-sha256' header does not match what was computed.",
		HttpStatusCode: http.StatusBadRequest,
	},

	/// Minio extensions.
	ErrStorageFull: {
		AwsErrorCode:           "XMinioStorageFull",
		Description:    "Storage backend has reached its minimum free disk threshold. Please delete few objects to proceed.",
		HttpStatusCode: http.StatusInternalServerError,
	},
	ErrObjectExistsAsDirectory: {
		AwsErrorCode:           "XMinioObjectExistsAsDirectory",
		Description:    "Object name already exists as a directory.",
		HttpStatusCode: http.StatusConflict,
	},
	ErrReadQuorum: {
		AwsErrorCode:           "XMinioReadQuorum",
		Description:    "Multiple disk failures, unable to reconstruct data.",
		HttpStatusCode: http.StatusServiceUnavailable,
	},
	ErrWriteQuorum: {
		AwsErrorCode:           "XMinioWriteQuorum",
		Description:    "Multiple disks failures, unable to write data.",
		HttpStatusCode: http.StatusServiceUnavailable,
	},
	ErrPolicyNesting: {
		AwsErrorCode:           "XMinioPolicyNesting",
		Description:    "Policy nesting conflict has occurred.",
		HttpStatusCode: http.StatusConflict,
	},
	ErrInvalidObjectName: {
		AwsErrorCode:           "XMinioInvalidObjectName",
		Description:    "Object name contains unsupported characters. Unsupported characters are `^*|\\\"",
		HttpStatusCode: http.StatusBadRequest,
	},
	// Add your error structure here.
}


func (e ApiErrorCode) AwsErrorCode() string {
	awsError, ok := ErrorCodeResponse[e]
	if !ok {
		return "InternalError"
	}
	return awsError.AwsErrorCode
}

func (e ApiErrorCode) Description() string {
	awsError, ok := ErrorCodeResponse[e]
	if !ok {
		return "We encountered an internal error, please try again."
	}
	return awsError.Description
}

func (e ApiErrorCode) Error() string {
	return e.Description()
}

func (e ApiErrorCode) HttpStatusCode() int {
	awsError, ok := ErrorCodeResponse[e]
	if !ok {
		return http.StatusInternalServerError
	}
	return awsError.HttpStatusCode
}
