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
	"encoding/hex"
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	. "github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/policy"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/signature"
)

// supportedGetReqParams - supported request parameters for GET presigned request.
var supportedGetReqParams = map[string]string{
	"response-expires":             "Expires",
	"response-content-type":        "Content-Type",
	"response-cache-control":       "Cache-Control",
	"response-content-disposition": "Content-Disposition",
	"response-content-language":    "Content-Language",
	"response-content-encoding":    "Content-Encoding",
}

// setGetRespHeaders - set any requested parameters as response headers.
func setGetRespHeaders(w http.ResponseWriter, reqParams url.Values) {
	for k, v := range reqParams {
		if header, ok := supportedGetReqParams[k]; ok {
			w.Header()[header] = v
		}
	}
}

// errAllowableNotFound - For an anon user, return 404 if have ListBucket, 403 otherwise
// this is in keeping with the permissions sections of the docs of both:
//   HEAD Object: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
//   GET Object: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
func (api ObjectAPIHandlers) errAllowableObjectNotFound(bucketName string,
	credential iam.Credential) error {

	bucket, err := api.ObjectAPI.GetBucket(bucketName)
	if err == ErrNoSuchBucket {
		return ErrNoSuchKey
	} else if err != nil {
		return ErrAccessDenied
	}
	switch bucket.ACL.CannedAcl {
	case "public-read", "public-read-write":
		return ErrNoSuchKey
	case "authenticated-read":
		if credential.AccessKeyID != "" {
			return ErrNoSuchKey
		} else {
			return ErrAccessDenied
		}
	default:
		if bucket.OwnerId == credential.UserId {
			return ErrNoSuchKey
		}
		return ErrAccessDenied
	}
}

// Simple way to convert a func to io.Writer type.
type funcToWriter func([]byte) (int, error)

func (f funcToWriter) Write(p []byte) (int, error) {
	return f(p)
}

// GetObjectHandler - GET Object
// ----------
// This implementation of the GET operation retrieves object. To use GET,
// you must have READ access to the object.
func (api ObjectAPIHandlers) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	var objectName, bucketName string
	vars := mux.Vars(r)
	bucketName = vars["bucket"]
	objectName = vars["object"]

	var credential iam.Credential
	var err error
	if credential, err = checkRequestAuth(api, r, policy.GetObjectAction, bucketName, objectName); err != nil {
		// As per "Permission" section in
		// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
		// If the object you request does not exist,
		// the error Amazon S3 returns depends on
		// whether you also have the s3:ListBucket
		// permission.
		// * If you have the s3:ListBucket permission
		//   on the bucket, Amazon S3 will return an
		//   HTTP status code 404 ("no such key")
		//   error.
		// * if you don’t have the s3:ListBucket
		//   permission, Amazon S3 will return an HTTP
		//   status code 403 ("access denied") error.`
		if signature.GetRequestAuthType(r) == signature.AuthTypeAnonymous {
			bucket, err := api.ObjectAPI.GetBucketInfo(bucketName, credential)
			if err != nil && err != ErrBucketAccessForbidden {
				WriteErrorResponse(w, r, err)
				return
			}
			if err == ErrBucketAccessForbidden {
				if bucket.Policy.IsAllowed(policy.Args{
					Action:          policy.ListBucketAction,
					BucketName:      bucketName,
					ConditionValues: getConditionValues(r, ""),
					IsOwner:         false,
				}) {
					WriteErrorResponse(w, r, ErrNoSuchKey)
					return
				}
			} else {
				WriteErrorResponse(w, r, ErrAccessDenied)
				return
			}
		}
	}

	version := r.URL.Query().Get("versionId")
	// Fetch object stat info.
	object, err := api.ObjectAPI.GetObjectInfo(bucketName, objectName, version, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to fetch object info.")
		if err == ErrNoSuchKey {
			err = api.errAllowableObjectNotFound(bucketName, credential)
		}
		WriteErrorResponse(w, r, err)
		return
	}

	if object.DeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
		WriteErrorResponse(w, r, ErrNoSuchKey)
		return
	}

	// Get request range.
	var hrange *HttpRange
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		if hrange, err = ParseRequestRange(rangeHeader, object.Size); err != nil {
			// Handle only ErrorInvalidRange
			// Ignore other parse error and treat it as regular Get request like Amazon S3.
			if err == ErrorInvalidRange {
				WriteErrorResponse(w, r, ErrInvalidRange)
				return
			}

			// log the error.
			helper.ErrorIf(err, "Invalid request range")
		}
	}

	// Validate pre-conditions if any.
	if err = checkPreconditions(r.Header, object); err != nil {
		// set object-related metadata headers
		w.Header().Set("Last-Modified", object.LastModifiedTime.UTC().Format(http.TimeFormat))

		if object.Etag != "" {
			w.Header()["ETag"] = []string{"\"" + object.Etag + "\""}
		}
		if err == ContentNotModified { // write only header if is a 304
			WriteErrorResponseHeaders(w, err)
		} else {
			WriteErrorResponse(w, r, err)
		}
		return
	}

	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	if len(sseRequest.CopySourceSseCustomerKey) != 0 {
		WriteErrorResponse(w, r, ErrInvalidSseHeader)
		return
	}

	// Get the object.
	startOffset := int64(0)
	length := object.Size
	if hrange != nil {
		startOffset = hrange.OffsetBegin
		length = hrange.GetLength()
	}
	// Indicates if any data was written to the http.ResponseWriter
	dataWritten := false
	// io.Writer type which keeps track if any data was written.
	writer := funcToWriter(func(p []byte) (int, error) {
		if !dataWritten {
			// Set headers on the first write.
			// Set standard object headers.
			SetObjectHeaders(w, object, hrange)

			// Set any additional requested response headers.
			setGetRespHeaders(w, r.URL.Query())

			if version != "" {
				w.Header().Set("x-amz-version-id", version)
			}

			dataWritten = true
		}
		return w.Write(p)
	})

	switch object.SseType {
	case "":
		break
	case "KMS":
		w.Header().Set("X-Amz-Server-Side-Encryption", "aws:kms")
		// TODO: not implemented yet
	case "S3":
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
	case "C":
		w.Header().Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
		w.Header().Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5",
			r.Header.Get("X-Amz-Server-Side-Encryption-Customer-Key-Md5"))
	}

	// Reads the object at startOffset and writes to mw.
	if err := api.ObjectAPI.GetObject(object, startOffset, length, writer, sseRequest); err != nil {
		helper.ErrorIf(err, "Unable to write to client.")
		if !dataWritten {
			// Error response only if no data has been written to client yet. i.e if
			// partial data has already been written before an error
			// occurred then no point in setting StatusCode and
			// sending error XML.
			WriteErrorResponse(w, r, err)
		}
		return
	}
	if !dataWritten {
		// If ObjectAPI.GetObject did not return error and no data has
		// been written it would mean that it is a 0-byte object.
		// call wrter.Write(nil) to set appropriate headers.
		writer.Write(nil)
	}
}

// HeadObjectHandler - HEAD Object
// -----------
// The HEAD operation retrieves metadata from an object without returning the object itself.
// TODO refactor HEAD and GET
func (api ObjectAPIHandlers) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {
	var objectName, bucketName string
	vars := mux.Vars(r)
	bucketName = vars["bucket"]
	objectName = vars["object"]

	var credential iam.Credential
	var err error
	if credential, err = checkRequestAuth(api, r, policy.GetObjectAction, bucketName, objectName); err != nil {
		// As per "Permission" section in
		// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
		// If the object you request does not exist,
		// the error Amazon S3 returns depends on
		// whether you also have the s3:ListBucket
		// permission.
		// * If you have the s3:ListBucket permission
		//   on the bucket, Amazon S3 will return an
		//   HTTP status code 404 ("no such key")
		//   error.
		// * if you don’t have the s3:ListBucket
		//   permission, Amazon S3 will return an HTTP
		//   status code 403 ("access denied") error.`
		if signature.GetRequestAuthType(r) == signature.AuthTypeAnonymous {
			bucket, err := api.ObjectAPI.GetBucketInfo(bucketName, credential)
			if err != nil && err != ErrBucketAccessForbidden {
				WriteErrorResponse(w, r, err)
				return
			}
			if err == ErrBucketAccessForbidden {
				if bucket.Policy.IsAllowed(policy.Args{
					Action:          policy.ListBucketAction,
					BucketName:      bucketName,
					ConditionValues: getConditionValues(r, ""),
					IsOwner:         false,
				}) {
					WriteErrorResponse(w, r, ErrNoSuchKey)
					return
				}
			} else {
				WriteErrorResponse(w, r, ErrAccessDenied)
				return
			}
		}
		WriteErrorResponse(w, r, err)
		return
	}

	version := r.URL.Query().Get("versionId")
	object, err := api.ObjectAPI.GetObjectInfo(bucketName, objectName, version, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to fetch object info.")
		if err == ErrNoSuchKey {
			err = api.errAllowableObjectNotFound(bucketName, credential)
		}
		WriteErrorResponse(w, r, err)
		return
	}

	if object.DeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
		WriteErrorResponse(w, r, ErrNoSuchKey)
		return
	}

	// Get request range.
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		if _, err = ParseRequestRange(rangeHeader, object.Size); err != nil {
			// Handle only ErrorInvalidRange
			// Ignore other parse error and treat it as regular Get request like Amazon S3.
			if err == ErrorInvalidRange {
				WriteErrorResponse(w, r, ErrInvalidRange)
				return
			}

			// log the error.
			helper.ErrorIf(err, "Invalid request range")
		}
	}

	// Validate pre-conditions if any.
	if err = checkPreconditions(r.Header, object); err != nil {
		// set object-related metadata headers
		w.Header().Set("Last-Modified", object.LastModifiedTime.UTC().Format(http.TimeFormat))

		if object.Etag != "" {
			w.Header()["ETag"] = []string{"\"" + object.Etag + "\""}
		}
		if err == ContentNotModified { // write only header if is a 304
			WriteErrorResponseHeaders(w, err)
		} else {
			WriteErrorResponse(w, r, err)
		}
		return
	}

	_, err = parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// Set standard object headers.
	SetObjectHeaders(w, object, nil)

	switch object.SseType {
	case "":
		break
	case "KMS":
		w.Header().Set("X-Amz-Server-Side-Encryption", "aws:kms")
		// TODO not implemented yet
	case "S3":
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
	case "C":
		w.Header().Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
		w.Header().Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5",
			r.Header.Get("X-Amz-Server-Side-Encryption-Customer-Key-Md5"))
	}

	// Successful response.
	w.WriteHeader(http.StatusOK)
}

// CopyObjectHandler - Copy Object
// ----------
// This implementation of the PUT operation adds an object to a bucket
// while reading the object from another source.
func (api ObjectAPIHandlers) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	targetBucketName := vars["bucket"]
	targetObjectName := vars["object"]

	var credential iam.Credential
	var err error
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}

	// TODO: Reject requests where body/payload is present, for now we don't even read it.

	// copy source is of form: /bucket-name/object-name?versionId=xxxxxx
	copySource := r.Header.Get("X-Amz-Copy-Source")

	// Skip the first element if it is '/', split the rest.
	if strings.HasPrefix(copySource, "/") {
		copySource = copySource[1:]
	}
	splits := strings.SplitN(copySource, "/", 2)

	// Save sourceBucket and sourceObject extracted from url Path.
	var sourceBucketName, sourceObjectName, sourceVersion string
	if len(splits) == 2 {
		sourceBucketName = splits[0]
		sourceObjectName = splits[1]
	}
	// If source object is empty, reply back error.
	if sourceObjectName == "" {
		WriteErrorResponse(w, r, ErrInvalidCopySource)
		return
	}

	splits = strings.SplitN(sourceObjectName, "?", 2)
	if len(splits) == 2 {
		sourceObjectName = splits[0]
		if !strings.HasPrefix(splits[1], "versionId=") {
			WriteErrorResponse(w, r, ErrInvalidCopySource)
			return
		}
		sourceVersion = strings.TrimPrefix(splits[1], "versionId=")
	}

	// X-Amz-Copy-Source should be URL-encoded
	sourceBucketName, err = url.QueryUnescape(sourceBucketName)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidCopySource)
		return
	}
	sourceObjectName, err = url.QueryUnescape(sourceObjectName)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidCopySource)
		return
	}

	if sourceBucketName == targetBucketName && sourceObjectName == targetObjectName {
		WriteErrorResponse(w, r, ErrInvalidCopyDest)
		return
	}

	helper.Debugln("sourceBucketName", sourceBucketName, "sourceObjectName", sourceObjectName,
		"sourceVersion", sourceVersion)

	sourceObject, err := api.ObjectAPI.GetObjectInfo(sourceBucketName, sourceObjectName,
		sourceVersion, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to fetch object info.")
		WriteErrorResponseWithResource(w, r, err, copySource)
		return
	}

	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// Verify before x-amz-copy-source preconditions before continuing with CopyObject.
	if err = checkObjectPreconditions(w, r, sourceObject); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	/// maximum Upload size for object in a single CopyObject operation.
	if isMaxObjectSize(sourceObject.Size) {
		WriteErrorResponseWithResource(w, r, ErrEntityTooLarge, copySource)
		return
	}

	pipeReader, pipeWriter := io.Pipe()
	go func() {
		startOffset := int64(0) // Read the whole file.
		// Get the object.
		err = api.ObjectAPI.GetObject(sourceObject, startOffset, sourceObject.Size,
			pipeWriter, sseRequest)
		if err != nil {
			helper.ErrorIf(err, "Unable to read an object.")
			pipeWriter.CloseWithError(err)
			return
		}
		pipeWriter.Close()
	}()

	targetAcl, err := getAclFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// Note that sourceObject and targetObject are pointers
	targetObject := &meta.Object{}
	targetObject.ACL = targetAcl
	targetObject.BucketName = targetBucketName
	targetObject.Name = targetObjectName
	targetObject.Size = sourceObject.Size
	targetObject.Etag = sourceObject.Etag
	targetObject.ContentType = sourceObject.ContentType
	targetObject.CustomAttributes = sourceObject.CustomAttributes
	targetObject.Parts = sourceObject.Parts

	// Create the object.
	result, err := api.ObjectAPI.CopyObject(targetObject, pipeReader, credential, sseRequest)
	if err != nil {
		helper.ErrorIf(err, "Unable to copy object from "+
			sourceObjectName+" to "+targetObjectName)
		WriteErrorResponse(w, r, err)
		return
	}

	response := GenerateCopyObjectResponse(result.Md5, result.LastModified)
	encodedSuccessResponse := EncodeResponse(response)
	// write headers
	if result.Md5 != "" {
		w.Header()["ETag"] = []string{"\"" + result.Md5 + "\""}
	}
	if sourceVersion != "" {
		w.Header().Set("x-amz-copy-source-version-id", sourceVersion)
	}
	if result.VersionId != "" {
		w.Header().Set("x-amz-version-id", result.VersionId)
	}
	// Set SSE related headers
	for _, headerName := range []string{
		"X-Amz-Server-Side-Encryption",
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
		"X-Amz-Server-Side-Encryption-Customer-Algorithm",
		"X-Amz-Server-Side-Encryption-Customer-Key-Md5",
	} {
		if header := r.Header.Get(headerName); header != "" {
			w.Header().Set(headerName, header)
		}
	}
	// write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
	// Explicitly close the reader, to avoid fd leaks.
	pipeReader.Close()
}

// PutObjectHandler - PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
func (api ObjectAPIHandlers) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	helper.Debugln("PutObjectHandler", "enter")
	// If the matching failed, it means that the X-Amz-Copy-Source was
	// wrong, fail right here.
	if _, ok := r.Header["X-Amz-Copy-Source"]; ok {
		WriteErrorResponse(w, r, ErrInvalidCopySource)
		return
	}
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	if !isValidObjectName(objectName) {
		WriteErrorResponse(w, r, ErrInvalidObjectName)
		return
	}

	// if Content-Length is unknown/missing, deny the request
	size := r.ContentLength
	if _, ok := r.Header["Content-Length"]; !ok {
		size = -1
	}
	if size == -1 && !contains(r.TransferEncoding, "chunked") {
		WriteErrorResponse(w, r, ErrMissingContentLength)
		return
	}
	// maximum Upload size for objects in a single operation
	if isMaxObjectSize(size) {
		WriteErrorResponse(w, r, ErrEntityTooLarge)
		return
	}

	// Save metadata.
	metadata := extractMetadataFromHeader(r.Header)
	// Get Content-Md5 sent by client and verify if valid
	if _, ok := r.Header["Content-Md5"]; !ok {
		metadata["md5Sum"] = ""
	} else {
		if len(r.Header.Get("Content-Md5")) == 0 {
			helper.Debugln("Content Md5 is null!")
			WriteErrorResponse(w, r, ErrInvalidDigest)
			return
		}
		md5Bytes, err := checkValidMD5(r.Header.Get("Content-Md5"))
		if err != nil {
			helper.Debugln("Content Md5 is invalid!")
			WriteErrorResponse(w, r, ErrInvalidDigest)
			return
		} else {
			metadata["md5Sum"] = hex.EncodeToString(md5Bytes)
		}
	}

	// Parse SSE related headers
	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	acl, err := getAclFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	credential, dataReader, err := signature.VerifyUpload(r)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	var result PutObjectResult
	result, err = api.ObjectAPI.PutObject(bucketName, objectName, credential, size, dataReader,
		metadata, acl, sseRequest)
	if err != nil {
		helper.ErrorIf(err, "Unable to create object "+objectName)
		WriteErrorResponse(w, r, err)
		return
	}

	if result.Md5 != "" {
		w.Header()["ETag"] = []string{"\"" + result.Md5 + "\""}
	}
	if result.VersionId != "" {
		w.Header().Set("x-amz-version-id", result.VersionId)
	}
	// Set SSE related headers
	for _, headerName := range []string{
		"X-Amz-Server-Side-Encryption",
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
		"X-Amz-Server-Side-Encryption-Customer-Algorithm",
		"X-Amz-Server-Side-Encryption-Customer-Key-Md5",
	} {
		if header := r.Header.Get(headerName); header != "" {
			w.Header().Set(headerName, header)
		}
	}
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) PutObjectAclHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	var credential iam.Credential
	var err error
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}
	var acl Acl
	var policy AccessControlPolicy
	if _, ok := r.Header["X-Amz-Acl"]; ok {
		acl, err = getAclFromHeader(r.Header)
	} else {
		aclBuffer, err := ioutil.ReadAll(io.LimitReader(r.Body, 1024))
		if err != nil {
			helper.ErrorIf(err, "Unable to read acls body")
			WriteErrorResponse(w, r, ErrInvalidAcl)
			return
		}
		err = xml.Unmarshal(aclBuffer, &policy)
		if err != nil {
			helper.ErrorIf(err, "Unable to Unmarshal xml for acl")
			WriteErrorResponse(w, r, ErrInternalError)
			return
		}
	}

	version := r.URL.Query().Get("versionId")
	err = api.ObjectAPI.SetObjectAcl(bucketName, objectName, version, policy, acl, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to set ACL for object")
		WriteErrorResponse(w, r, err)
		return
	}
	if version != "" {
		w.Header().Set("x-amz-version-id", version)
	}
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) GetObjectAclHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	var credential iam.Credential
	var err error
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}

	version := r.URL.Query().Get("versionId")
	policy, err := api.ObjectAPI.GetObjectAcl(bucketName, objectName, version, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to fetch object policy.")
		WriteErrorResponse(w, r, err)
		return
	}

	aclBuffer, err := xml.Marshal(policy)
	if err != nil {
		helper.ErrorIf(err, "Failed to marshal acl XML for object", objectName)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	if version != "" {
		w.Header().Set("x-amz-version-id", version)
	}
	WriteSuccessResponse(w, aclBuffer)
}

/// Multipart objectAPIHandlers

// NewMultipartUploadHandler - New multipart upload
func (api ObjectAPIHandlers) NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	if !isValidObjectName(objectName) {
		WriteErrorResponse(w, r, ErrInvalidObjectName)
		return
	}

	var credential iam.Credential
	var err error
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}

	acl, err := getAclFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// Save metadata.
	metadata := extractMetadataFromHeader(r.Header)

	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	uploadID, err := api.ObjectAPI.NewMultipartUpload(credential, bucketName, objectName,
		metadata, acl, sseRequest)
	if err != nil {
		helper.ErrorIf(err, "Unable to initiate new multipart upload id.")
		WriteErrorResponse(w, r, err)
		return
	}

	response := GenerateInitiateMultipartUploadResponse(bucketName, objectName, uploadID)
	encodedSuccessResponse := EncodeResponse(response)
	// Set SSE related headers
	for _, headerName := range []string{
		"X-Amz-Server-Side-Encryption",
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
		"X-Amz-Server-Side-Encryption-Customer-Algorithm",
		"X-Amz-Server-Side-Encryption-Customer-Key-Md5",
	} {
		if header := r.Header.Get(headerName); header != "" {
			w.Header().Set(headerName, header)
		}
	}
	// write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
}

// PutObjectPartHandler - Upload part
func (api ObjectAPIHandlers) PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	var incomingMd5 string
	// get Content-Md5 sent by client and verify if valid
	md5Bytes, err := checkValidMD5(r.Header.Get("Content-Md5"))
	if err != nil {
		incomingMd5 = ""
	} else {
		incomingMd5 = hex.EncodeToString(md5Bytes)
	}

	/// if Content-Length is unknown/missing, throw away
	size := r.ContentLength
	if size == -1 {
		WriteErrorResponse(w, r, ErrMissingContentLength)
		return
	}

	/// maximum Upload size for multipart objects in a single operation
	if isMaxObjectSize(size) {
		WriteErrorResponse(w, r, ErrEntityTooLarge)
		return
	}

	uploadID := r.URL.Query().Get("uploadId")
	partIDString := r.URL.Query().Get("partNumber")

	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidPart)
		return
	}

	// check partID with maximum part ID for multipart objects
	if isMaxPartID(partID) {
		WriteErrorResponse(w, r, ErrInvalidMaxParts)
		return
	}

	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidSseHeader)
		return
	}

	credential, dataReader, err := signature.VerifyUpload(r)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	var result PutObjectPartResult
	// No need to verify signature, anonymous request access is already allowed.
	result, err = api.ObjectAPI.PutObjectPart(bucketName, objectName, credential,
		uploadID, partID, size, dataReader, incomingMd5, sseRequest)
	if err != nil {
		helper.ErrorIf(err, "Unable to create object part for "+objectName)
		// Verify if the underlying error is signature mismatch.
		WriteErrorResponse(w, r, err)
		return
	}

	if result.ETag != "" {
		w.Header()["ETag"] = []string{"\"" + result.ETag + "\""}
	}
	switch result.SseType {
	case "":
		break
	case "KMS":
		w.Header().Set("X-Amz-Server-Side-Encryption", "aws:kms")
		w.Header().Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
			result.SseAwsKmsKeyIdBase64)
	case "S3":
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
	case "C":
		w.Header().Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
		w.Header().Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5",
			result.SseCustomerKeyMd5Base64)
	}
	WriteSuccessResponse(w, nil)
}

// Upload part - copy
func (api ObjectAPIHandlers) CopyObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	targetBucketName := vars["bucket"]
	targetObjectName := vars["object"]

	if !isValidObjectName(targetObjectName) {
		WriteErrorResponse(w, r, ErrInvalidObjectName)
		return
	}

	var credential iam.Credential
	var err error
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}

	targetUploadId := r.URL.Query().Get("uploadId")
	partIdString := r.URL.Query().Get("partNumber")

	targetPartId, err := strconv.Atoi(partIdString)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidPart)
		return
	}

	// check partID with maximum part ID for multipart objects
	if isMaxPartID(targetPartId) {
		WriteErrorResponse(w, r, ErrInvalidMaxParts)
		return
	}

	// copy source is of form: /bucket-name/object-name?versionId=xxxxxx
	copySource := r.Header.Get("X-Amz-Copy-Source")

	// Skip the first element if it is '/', split the rest.
	if strings.HasPrefix(copySource, "/") {
		copySource = copySource[1:]
	}
	splits := strings.SplitN(copySource, "/", 2)

	// Save sourceBucket and sourceObject extracted from url Path.
	var sourceBucketName, sourceObjectName, sourceVersion string
	if len(splits) == 2 {
		sourceBucketName = splits[0]
		sourceObjectName = splits[1]
	}
	// If source object is empty, reply back error.
	if sourceObjectName == "" {
		WriteErrorResponse(w, r, ErrInvalidCopySource)
		return
	}

	splits = strings.SplitN(sourceObjectName, "?", 2)
	if len(splits) == 2 {
		sourceObjectName = splits[0]
		if !strings.HasPrefix(splits[1], "versionId=") {
			WriteErrorResponse(w, r, ErrInvalidCopySource)
			return
		}
		sourceVersion = strings.TrimPrefix(splits[1], "versionId=")
	}

	// X-Amz-Copy-Source should be URL-encoded
	sourceBucketName, err = url.QueryUnescape(sourceBucketName)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidCopySource)
		return
	}
	sourceObjectName, err = url.QueryUnescape(sourceObjectName)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidCopySource)
		return
	}

	sourceObject, err := api.ObjectAPI.GetObjectInfo(sourceBucketName, sourceObjectName,
		sourceVersion, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to fetch object info.")
		WriteErrorResponseWithResource(w, r, err, copySource)
		return
	}

	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponseWithResource(w, r, err, copySource)
		return
	}

	// Verify before x-amz-copy-source preconditions before continuing with CopyObject.
	if err = checkObjectPreconditions(w, r, sourceObject); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	var readOffset, readLength int64
	copySourceRangeString := r.Header.Get("x-amz-copy-source-range")
	if copySourceRangeString == "" {
		readOffset = 0
		readLength = sourceObject.Size
	} else {
		copySourceRange, err := ParseRequestRange(copySourceRangeString, sourceObject.Size)
		if err != nil {
			helper.ErrorIf(err, "Invalid request range")
			WriteErrorResponse(w, r, ErrInvalidRange)
			return
		}
		readOffset = copySourceRange.OffsetBegin
		readLength = copySourceRange.GetLength()
		if isMaxObjectSize(copySourceRange.OffsetEnd - copySourceRange.OffsetBegin + 1) {
			WriteErrorResponseWithResource(w, r, ErrEntityTooLarge, copySource)
			return
		}
	}
	if isMaxObjectSize(readLength) {
		WriteErrorResponseWithResource(w, r, ErrEntityTooLarge, copySource)
		return
	}

	pipeReader, pipeWriter := io.Pipe()
	defer pipeReader.Close()
	go func() {
		err = api.ObjectAPI.GetObject(sourceObject, readOffset, readLength,
			pipeWriter, sseRequest)
		if err != nil {
			helper.ErrorIf(err, "Unable to read an object.")
			pipeWriter.CloseWithError(err)
			return
		}
		pipeWriter.Close()
	}()

	// Create the object.
	result, err := api.ObjectAPI.CopyObjectPart(targetBucketName, targetObjectName, targetUploadId,
		targetPartId, readLength, pipeReader, credential, sseRequest)
	if err != nil {
		helper.ErrorIf(err, "Unable to copy object part from "+sourceObjectName+
			" to "+targetObjectName)
		WriteErrorResponse(w, r, err)
		return
	}

	response := GenerateCopyObjectPartResponse(result.Md5, result.LastModified)
	encodedSuccessResponse := EncodeResponse(response)
	// write headers
	if result.Md5 != "" {
		w.Header()["ETag"] = []string{"\"" + result.Md5 + "\""}
	}
	if sourceVersion != "" {
		w.Header().Set("x-amz-copy-source-version-id", sourceVersion)
	}
	// Set SSE related headers
	for _, headerName := range []string{
		"X-Amz-Server-Side-Encryption",
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
		"X-Amz-Server-Side-Encryption-Customer-Algorithm",
		"X-Amz-Server-Side-Encryption-Customer-Key-Md5",
	} {
		if header := r.Header.Get(headerName); header != "" {
			w.Header().Set(headerName, header)
		}
	}
	// write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
}

// AbortMultipartUploadHandler - Abort multipart upload
func (api ObjectAPIHandlers) AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	var credential iam.Credential
	var err error
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}

	uploadId := r.URL.Query().Get("uploadId")
	if err := api.ObjectAPI.AbortMultipartUpload(credential, bucketName,
		objectName, uploadId); err != nil {

		helper.ErrorIf(err, "Unable to abort multipart upload.")
		WriteErrorResponse(w, r, err)
		return
	}
	WriteSuccessNoContent(w)
}

// ListObjectPartsHandler - List object parts
func (api ObjectAPIHandlers) ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	var credential iam.Credential
	var err error
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}

	request, err := parseListObjectPartsQuery(r.URL.Query())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	listPartsInfo, err := api.ObjectAPI.ListObjectParts(credential, bucketName,
		objectName, request)
	if err != nil {
		helper.ErrorIf(err, "Unable to list uploaded parts.")
		WriteErrorResponse(w, r, err)
		return
	}
	encodedSuccessResponse := EncodeResponse(listPartsInfo)
	// Write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
}

// CompleteMultipartUploadHandler - Complete multipart upload
func (api ObjectAPIHandlers) CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	// Get upload id.
	uploadId := r.URL.Query().Get("uploadId")

	var credential iam.Credential
	var err error
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}
	completeMultipartBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		helper.ErrorIf(err, "Unable to complete multipart upload.")
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
	complMultipartUpload := &meta.CompleteMultipartUpload{}
	if err = xml.Unmarshal(completeMultipartBytes, complMultipartUpload); err != nil {
		helper.ErrorIf(err, "Unable to parse complete multipart upload XML.")
		WriteErrorResponse(w, r, ErrMalformedXML)
		return
	}
	if len(complMultipartUpload.Parts) == 0 {
		WriteErrorResponse(w, r, ErrMalformedXML)
		return
	}
	if !sort.IsSorted(meta.CompletedParts(complMultipartUpload.Parts)) {
		WriteErrorResponse(w, r, ErrInvalidPartOrder)
		return
	}
	// Complete parts.
	var completeParts []meta.CompletePart
	for _, part := range complMultipartUpload.Parts {
		part.ETag = strings.TrimPrefix(part.ETag, "\"")
		part.ETag = strings.TrimSuffix(part.ETag, "\"")
		completeParts = append(completeParts, part)
	}

	var result CompleteMultipartResult
	result, err = api.ObjectAPI.CompleteMultipartUpload(credential, bucketName,
		objectName, uploadId, completeParts)

	if err != nil {
		helper.ErrorIf(err, "Unable to complete multipart upload.")
		switch oErr := err.(type) {
		case meta.PartTooSmall:
			// Write part too small error.
			writePartSmallErrorResponse(w, r, oErr)
		default:
			// Handle all other generic issues.
			WriteErrorResponse(w, r, err)
		}
		return
	}

	// Get object location.
	location := GetLocation(r)
	// Generate complete multipart response.
	response := GenerateCompleteMultpartUploadResponse(bucketName, objectName, location, result.ETag)
	encodedSuccessResponse, err := xml.Marshal(response)
	if err != nil {
		helper.ErrorIf(err, "Unable to parse CompleteMultipartUpload response")
		WriteErrorResponseNoHeader(w, r, ErrInternalError, r.URL.Path)
		return
	}
	if result.VersionId != "" {
		w.Header().Set("x-amz-version-id", result.VersionId)
	}
	switch result.SseType {
	case "":
		break
	case "KMS":
		w.Header().Set("X-Amz-Server-Side-Encryption", "aws:kms")
		w.Header().Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
			result.SseAwsKmsKeyIdBase64)
	case "S3":
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
	case "C":
		w.Header().Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
		w.Header().Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5",
			result.SseCustomerKeyMd5Base64)
	}
	// write success response.
	w.WriteHeader(http.StatusOK)
	w.Write(encodedSuccessResponse)
}

/// Delete objectAPIHandlers

// DeleteObjectHandler - delete an object
func (api ObjectAPIHandlers) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	var credential iam.Credential
	var err error
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypeSignedV4, signature.AuthTypePresignedV4,
		signature.AuthTypeSignedV2, signature.AuthTypePresignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}
	version := r.URL.Query().Get("versionId")
	/// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
	/// Ignore delete object errors, since we are supposed to reply
	/// only 204.
	result, err := api.ObjectAPI.DeleteObject(bucketName, objectName, version, credential)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	if result.DeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
	} else {
		w.Header().Set("x-amz-delete-marker", "false")
	}
	if result.VersionId != "" {
		w.Header().Set("x-amz-version-id", result.VersionId)
	}
	WriteSuccessNoContent(w)
}
