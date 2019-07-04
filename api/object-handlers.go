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
	"errors"
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
	"github.com/journeymidnight/yig/crypto"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
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

func getStorageClassFromHeader(r *http.Request) (meta.StorageClass, error) {
	storageClassStr := r.Header.Get("X-Amz-Storage-Class")
	if storageClassStr != "" {
		helper.Logger.Println(20, "Get storage class header:", storageClassStr)
		return meta.MatchStorageClassIndex(storageClassStr)
	} else {
		// If you don't specify this header, Amazon S3 uses STANDARD
		return meta.ObjectStorageClassStandard, nil
	}
}

// errAllowableNotFound - For an anon user, return 404 if have ListBucket, 403 otherwise
// this is in keeping with the permissions sections of the docs of both:
//   HEAD Object: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
//   GET Object: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
func (api ObjectAPIHandlers) errAllowableObjectNotFound(request *http.Request, bucketName string,
	credential common.Credential) error {

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

	bucket, err := api.ObjectAPI.GetBucket(bucketName)
	if err != nil {
		return err
	}

	if bucket.Policy.IsAllowed(policy.Args{
		Action:          policy.ListBucketAction,
		BucketName:      bucketName,
		ConditionValues: getConditionValues(request, ""),
		IsOwner:         false,
	}) == policy.PolicyAllow {
		return ErrNoSuchKey
	} else {
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

	var credential common.Credential
	var err error
	if _, credential, err = checkRequestAuth(api, r, policy.GetObjectAction, bucketName, objectName); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	version := r.URL.Query().Get("versionId")
	// Fetch object stat info.
	object, err := api.ObjectAPI.GetObjectInfo(bucketName, objectName, version, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to fetch object info.")
		if err == ErrNoSuchKey {
			err = api.errAllowableObjectNotFound(r, bucketName, credential)
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
	case crypto.S3KMS.String():
		w.Header().Set("X-Amz-Server-Side-Encryption", "aws:kms")
		// TODO: not implemented yet
	case crypto.S3.String():
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
	case crypto.SSEC.String():
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

	var credential common.Credential
	var err error
	if _, credential, err = checkRequestAuth(api, r, policy.GetObjectAction, bucketName, objectName); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	version := r.URL.Query().Get("versionId")
	object, err := api.ObjectAPI.GetObjectInfo(bucketName, objectName, version, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to fetch object info.")
		if err == ErrNoSuchKey {
			err = api.errAllowableObjectNotFound(r, bucketName, credential)
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
	case crypto.S3KMS.String():
		w.Header().Set("X-Amz-Server-Side-Encryption", "aws:kms")
		// TODO not implemented yet
	case crypto.S3.String():
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
	case crypto.SSEC.String():
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
	helper.Logger.Println(20, "CopyObjectHandler enter")
	vars := mux.Vars(r)
	targetBucketName := vars["bucket"]
	targetObjectName := vars["object"]

	var credential common.Credential
	var err error
	if _, credential, err = checkRequestAuth(api, r, policy.PutObjectAction, targetBucketName, targetObjectName); err != nil {
		WriteErrorResponse(w, r, err)
		return
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

	var isOnlyUpdateMetadata = false

	if sourceBucketName == targetBucketName && sourceObjectName == targetObjectName {
		if r.Header.Get("X-Amz-Metadata-Directive") == "COPY" {
			WriteErrorResponse(w, r, ErrInvalidCopyDest)
			return
		} else if r.Header.Get("X-Amz-Metadata-Directive") == "REPLACE" {
			isOnlyUpdateMetadata = true
		} else {
			WriteErrorResponse(w, r, ErrInvalidRequestBody)
			return
		}
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

	//TODO: In a versioning-enabled bucket, you cannot change the storage class of a specific version of an object. When you copy it, Amazon S3 gives it a new version ID.
	storageClassFromHeader, err := getStorageClassFromHeader(r)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	if storageClassFromHeader == meta.ObjectStorageClassGlacier || storageClassFromHeader == meta.ObjectStorageClassDeepArchive {
		WriteErrorResponse(w, r, ErrInvalidCopySourceStorageClass)
		return
	}

	//if source == dest and X-Amz-Metadata-Directive == REPLACE, only update the meta;
	if isOnlyUpdateMetadata {
		targetObject := sourceObject

		//update custom attrs from headers
		newMetadata := extractMetadataFromHeader(r.Header)
		if c, ok := newMetadata["Content-Type"]; ok {
			targetObject.ContentType = c
		} else {
			targetObject.ContentType = sourceObject.ContentType
		}
		targetObject.CustomAttributes = newMetadata
		targetObject.StorageClass = storageClassFromHeader

		result, err := api.ObjectAPI.UpdateObjectAttrs(targetObject, credential)
		if err != nil {
			helper.ErrorIf(err, "Unable to update object meta for "+targetObject.ObjectId)
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

	targetACL, err := getAclFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// Note that sourceObject and targetObject are pointers
	targetObject := &meta.Object{}
	targetObject.ACL = targetACL
	targetObject.BucketName = targetBucketName
	targetObject.Name = targetObjectName
	targetObject.Size = sourceObject.Size
	targetObject.Etag = sourceObject.Etag
	targetObject.ContentType = sourceObject.ContentType
	targetObject.CustomAttributes = sourceObject.CustomAttributes
	targetObject.Parts = sourceObject.Parts
	if r.Header.Get("X-Amz-Storage-Class") != "" {
		targetObject.StorageClass = storageClassFromHeader
	} else {
		targetObject.StorageClass = sourceObject.StorageClass
	}

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

	var authType = signature.GetRequestAuthType(r)
	var err error
	if !isValidObjectName(objectName) {
		WriteErrorResponse(w, r, ErrInvalidObjectName)
		return
	}

	// if Content-Length is unknown/missing, deny the request
	size := r.ContentLength
	if authType == signature.AuthTypeStreamingSigned {
		if sizeStr, ok := r.Header["X-Amz-Decoded-Content-Length"]; ok {
			if sizeStr[0] == "" {
				WriteErrorResponse(w, r, ErrMissingContentLength)
				return
			}
			size, err = strconv.ParseInt(sizeStr[0], 10, 64)
			if err != nil {
				WriteErrorResponse(w, r, err)
				return
			}
		}
	}

	if size == -1 {
		WriteErrorResponse(w, r, ErrMissingContentLength)
		return
	}

	// maximum Upload size for objects in a single operation
	if isMaxObjectSize(size) {
		WriteErrorResponse(w, r, ErrEntityTooLarge)
		return
	}

	storageClass, err := getStorageClassFromHeader(r)
	if err != nil {
		WriteErrorResponse(w, r, err)
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

	if authType == signature.AuthTypeStreamingSigned {
		if contentEncoding, ok := metadata["content-encoding"]; ok {
			contentEncoding = signature.TrimAwsChunkedContentEncoding(contentEncoding)
			if contentEncoding != "" {
				// Make sure to trim and save the content-encoding
				// parameter for a streaming signature which is set
				// to a custom value for example: "aws-chunked,gzip".
				metadata["content-encoding"] = contentEncoding
			} else {
				// Trimmed content encoding is empty when the header
				// value is set to "aws-chunked" only.

				// Make sure to delete the content-encoding parameter
				// for a streaming signature which is set to value
				// for example: "aws-chunked"
				delete(metadata, "content-encoding")
			}
		}
	}

	// Parse SSE related headers
	// Suport SSE-S3 and SSE-C now
	var sseRequest SseRequest

	if hasServerSideEncryptionHeader(r.Header) && !hasSuffix(objectName, "/") { // handle SSE requests
		sseRequest, err = parseSseHeader(r.Header)
		if err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
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
		metadata, acl, sseRequest, storageClass)
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

// AppendObjectHandler - Append Object
// ----------
// This implementation of the POST operation append an object in a bucket.
func (api ObjectAPIHandlers) AppendObjectHandler(w http.ResponseWriter, r *http.Request) {
	helper.Debugln("AppendObjectHandler", "enter")

	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	var authType = signature.GetRequestAuthType(r)
	var err error

	if !isValidObjectName(objectName) {
		WriteErrorResponse(w, r, ErrInvalidObjectName)
		return
	}

	pos := r.URL.Query().Get("position")
	// Parse SSE related headers
	// Suport SSE-S3 and SSE-C now
	var sseRequest SseRequest
	var position uint64
	var acl Acl

	if position, err = checkPosition(pos); err != nil {
		WriteErrorResponse(w, r, ErrInvalidPosition)
		return
	}

	// if Content-Length is unknown/missing, deny the request
	// if Content-Length is unknown/missing, deny the request
	size := r.ContentLength
	if authType == signature.AuthTypeStreamingSigned {
		if sizeStr, ok := r.Header["X-Amz-Decoded-Content-Length"]; ok {
			if sizeStr[0] == "" {
				WriteErrorResponse(w, r, ErrMissingContentLength)
				return
			}
			size, err = strconv.ParseInt(sizeStr[0], 10, 64)
			if err != nil {
				WriteErrorResponse(w, r, err)
				return
			}
		}
	}
	if size == -1 {
		WriteErrorResponse(w, r, ErrMissingContentLength)
		return
	}

	// maximum Upload size for objects in a single operation
	if isMaxObjectSize(size) {
		WriteErrorResponse(w, r, ErrEntityTooLarge)
		return
	}

	storageClass, err := getStorageClassFromHeader(r)
	if err != nil {
		WriteErrorResponse(w, r, err)
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

	if authType == signature.AuthTypeStreamingSigned {
		if contentEncoding, ok := metadata["content-encoding"]; ok {
			contentEncoding = signature.TrimAwsChunkedContentEncoding(contentEncoding)
			if contentEncoding != "" {
				// Make sure to trim and save the content-encoding
				// parameter for a streaming signature which is set
				// to a custom value for example: "aws-chunked,gzip".
				metadata["content-encoding"] = contentEncoding
			} else {
				// Trimmed content encoding is empty when the header
				// value is set to "aws-chunked" only.

				// Make sure to delete the content-encoding parameter
				// for a streaming signature which is set to value
				// for example: "aws-chunked"
				delete(metadata, "content-encoding")
			}
		}
	}

	// Verify auth
	credential, dataReader, err := signature.VerifyUpload(r)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// Check whether the object is exist or not
	// Check whether the bucket is owned by the specified user
	objInfo, err := api.ObjectAPI.GetObjectInfo(bucketName, objectName, "", credential)
	if err != nil && err != ErrNoSuchKey {
		WriteErrorResponse(w, r, err)
		return
	}

	if objInfo != nil && objInfo.Type != meta.ObjectTypeAppendable {
		WriteErrorResponse(w, r, ErrObjectNotAppendable)
		return
	}

	if objInfo != nil && objInfo.Size != int64(position) {
		helper.Debugln("Current Size:", objInfo.Size, "Position:", position)
		w.Header().Set("X-Amz-Next-Append-Position", strconv.FormatInt(objInfo.Size, 10))
		WriteErrorResponse(w, r, ErrPositionNotEqualToLength)
		return
	}

	if err == ErrNoSuchKey {
		if isFirstAppend(position) {
			acl, err = getAclFromHeader(r.Header)
			if err != nil {
				WriteErrorResponse(w, r, err)
				return
			}
		} else {
			w.Header().Set("X-Amz-Next-Append-Position", "0")
			WriteErrorResponse(w, r, ErrPositionNotEqualToLength)
			return
		}
	} else {
		acl = objInfo.ACL
	}

	if hasServerSideEncryptionHeader(r.Header) && !hasSuffix(objectName, "/") { // handle SSE requests
		sseRequest, err = parseSseHeader(r.Header)
		if err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
		if sseRequest.Type == crypto.SSEC.String() {
			WriteErrorResponse(w, r, ErrNotImplemented)
			return
		}
	}

	var result AppendObjectResult
	result, err = api.ObjectAPI.AppendObject(bucketName, objectName, credential, position, size, dataReader,
		metadata, acl, sseRequest, storageClass, objInfo)
	if err != nil {
		helper.ErrorIf(err, "Unable to append object "+objectName)
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
		//"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
		//"X-Amz-Server-Side-Encryption-Customer-Algorithm",
		//"X-Amz-Server-Side-Encryption-Customer-Key-Md5",
	} {
		if header := r.Header.Get(headerName); header != "" {
			w.Header().Set(headerName, header)
		}
	}

	// Set next position
	w.Header().Set("X-Amz-Next-Append-Position", strconv.FormatInt(result.NextPosition, 10))
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) PutObjectAclHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	var credential common.Credential
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
		if err != nil {
			WriteErrorResponse(w, r, ErrInvalidAcl)
			return
		}
	} else {
		aclBuffer, err := ioutil.ReadAll(io.LimitReader(r.Body, 1024))
		helper.Debug("acl body:\n %s", string(aclBuffer))
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

	var credential common.Credential
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

	aclBuffer, err := xmlFormat(policy)
	if err != nil {
		helper.ErrorIf(err, "Failed to marshal acl XML for object", objectName)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	if version != "" {
		w.Header().Set("x-amz-version-id", version)
	}

	setXmlHeader(w, aclBuffer)
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

	var credential common.Credential
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

	var sseRequest SseRequest
	if hasServerSideEncryptionHeader(r.Header) && !hasSuffix(objectName, "/") { // handle SSE requests
		sseRequest, err = parseSseHeader(r.Header)
		if err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}

	storageClass, err := getStorageClassFromHeader(r)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	uploadID, err := api.ObjectAPI.NewMultipartUpload(credential, bucketName, objectName,
		metadata, acl, sseRequest, storageClass)
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

	authType := signature.GetRequestAuthType(r)

	var incomingMd5 string
	// get Content-Md5 sent by client and verify if valid
	md5Bytes, err := checkValidMD5(r.Header.Get("Content-Md5"))
	if err != nil {
		incomingMd5 = ""
	} else {
		incomingMd5 = hex.EncodeToString(md5Bytes)
	}

	size := r.ContentLength
	if authType == signature.AuthTypeStreamingSigned {
		if sizeStr, ok := r.Header["X-Amz-Decoded-Content-Length"]; ok {
			if sizeStr[0] == "" {
				WriteErrorResponse(w, r, ErrMissingContentLength)
				return
			}
			size, err = strconv.ParseInt(sizeStr[0], 10, 64)
			if err != nil {
				WriteErrorResponse(w, r, err)
				return
			}
		}
	}

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
	case crypto.S3KMS.String():
		w.Header().Set("X-Amz-Server-Side-Encryption", "aws:kms")
		w.Header().Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
			result.SseAwsKmsKeyIdBase64)
	case crypto.S3.String():
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
	case crypto.SSEC.String():
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

	var credential common.Credential
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

	var credential common.Credential
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

	var credential common.Credential
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

	var credential common.Credential
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
		helper.ErrorIf(err, "Unable to complete multipart upload when read request body.")
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
	complMultipartUpload := &meta.CompleteMultipartUpload{}
	if err = xml.Unmarshal(completeMultipartBytes, complMultipartUpload); err != nil {
		helper.ErrorIf(err, "Unable to parse complete multipart upload XML. data: %s", string(completeMultipartBytes))
		WriteErrorResponse(w, r, ErrMalformedXML)
		return
	}
	if len(complMultipartUpload.Parts) == 0 {
		helper.ErrorIf(errors.New("len(complMultipartUpload.Parts) == 0"), "Unable to complete multipart upload.")
		WriteErrorResponse(w, r, ErrMalformedXML)
		return
	}
	if !sort.IsSorted(meta.CompletedParts(complMultipartUpload.Parts)) {
		helper.ErrorIf(errors.New("part not sorted."), "Unable to complete multipart upload. data: %+v", complMultipartUpload.Parts)
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
	encodedSuccessResponse, err := xmlFormat(response)
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
	case crypto.S3KMS.String():
		w.Header().Set("X-Amz-Server-Side-Encryption", "aws:kms")
		w.Header().Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id",
			result.SseAwsKmsKeyIdBase64)
	case crypto.S3.String():
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
	case crypto.SSEC.String():
		w.Header().Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
		w.Header().Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5",
			result.SseCustomerKeyMd5Base64)
	}

	setXmlHeader(w, encodedSuccessResponse)
	// write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
}

/// Delete objectAPIHandlers

// DeleteObjectHandler - delete an object
func (api ObjectAPIHandlers) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	var credential common.Credential
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

// PostPolicyBucketHandler - POST policy upload
// ----------
// This implementation of the POST operation handles object creation with a specified
// signature policy in multipart/form-data

var ValidSuccessActionStatus = []string{"200", "201", "204"}

func (api ObjectAPIHandlers) PostObjectHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary files.
	reader, err := r.MultipartReader()
	if err != nil {
		helper.ErrorIf(err, "Unable to initialize multipart reader.")
		WriteErrorResponse(w, r, ErrMalformedPOSTRequest)
		return
	}

	fileBody, formValues, err := extractHTTPFormValues(reader)
	if err != nil {
		helper.ErrorIf(err, "Unable to parse form values.")
		WriteErrorResponse(w, r, ErrMalformedPOSTRequest)
		return
	}
	objectName := formValues["Key"]
	if !isValidObjectName(objectName) {
		WriteErrorResponse(w, r, ErrInvalidObjectName)
		return
	}

	bucketName := mux.Vars(r)["bucket"]
	formValues["Bucket"] = bucketName
	bucket, err := api.ObjectAPI.GetBucket(bucketName)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	helper.Debugln("formValues", formValues)
	helper.Debugln("bucket", bucketName)

	var credential common.Credential
	postPolicyType := signature.GetPostPolicyType(formValues)
	helper.Debugln("type", postPolicyType)
	switch postPolicyType {
	case signature.PostPolicyV2:
		credential, err = signature.DoesPolicySignatureMatchV2(formValues)
	case signature.PostPolicyV4:
		credential, err = signature.DoesPolicySignatureMatchV4(formValues)
	case signature.PostPolicyAnonymous:
		if bucket.ACL.CannedAcl != "public-read-write" {
			WriteErrorResponse(w, r, ErrAccessDenied)
			return
		}
	default:
		WriteErrorResponse(w, r, ErrMalformedPOSTRequest)
		return
	}
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	if err = signature.CheckPostPolicy(formValues, postPolicyType); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// Convert form values to header type so those values could be handled as in
	// normal requests
	headerfiedFormValues := make(http.Header)
	for key := range formValues {
		headerfiedFormValues.Add(key, formValues[key])
	}

	metadata := extractMetadataFromHeader(headerfiedFormValues)

	var acl Acl
	acl.CannedAcl = headerfiedFormValues.Get("acl")
	if acl.CannedAcl == "" {
		acl.CannedAcl = "private"
	}
	err = IsValidCannedAcl(acl)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidCannedAcl)
		return
	}

	sseRequest, err := parseSseHeader(headerfiedFormValues)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	storageClass, err := getStorageClassFromHeader(r)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	result, err := api.ObjectAPI.PutObject(bucketName, objectName, credential, -1, fileBody,
		metadata, acl, sseRequest, storageClass)
	if err != nil {
		helper.ErrorIf(err, "Unable to create object "+objectName)
		WriteErrorResponse(w, r, err)
		return
	}
	if result.Md5 != "" {
		w.Header().Set("ETag", "\""+result.Md5+"\"")
	}

	var redirect string
	redirect, _ = formValues["Success_action_redirect"]
	if redirect == "" {
		redirect, _ = formValues["redirect"]
	}
	if redirect != "" {
		redirectUrl, err := url.Parse(redirect)
		if err == nil {
			redirectUrl.Query().Set("bucket", bucketName)
			redirectUrl.Query().Set("key", objectName)
			redirectUrl.Query().Set("etag", result.Md5)
			http.Redirect(w, r, redirectUrl.String(), http.StatusSeeOther)
			return
		}
		// If URL is Invalid, ignore the redirect field
	}

	var status string
	status, _ = formValues["Success_action_status"]
	if !helper.StringInSlice(status, ValidSuccessActionStatus) {
		status = "204"
	}

	statusCode, _ := strconv.Atoi(status)
	switch statusCode {
	case 200, 204:
		w.WriteHeader(statusCode)
	case 201:
		encodedSuccessResponse := EncodeResponse(PostResponse{
			Location: GetObjectLocation(bucketName, objectName), // TODO Full URL is preferred
			Bucket:   bucketName,
			Key:      objectName,
			ETag:     result.Md5,
		})
		w.WriteHeader(201)
		w.Write(encodedSuccessResponse)
	}
}
