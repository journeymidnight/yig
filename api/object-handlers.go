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
	"time"

	. "git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/iam"
	"git.letv.cn/yig/yig/meta"
	"git.letv.cn/yig/yig/signature"
	mux "github.com/gorilla/mux"
)

// supportedGetReqParams - supported request parameters for GET presigned request.
var supportedGetReqParams = map[string]string{
	"response-expires":             "Expires",
	"response-content-type":        "Content-Type",
	"response-cache-control":       "Cache-Control",
	"response-content-disposition": "Content-Disposition",
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
func errAllowableObjectNotFound(bucket string, r *http.Request) error {
	if signature.GetRequestAuthType(r) == signature.AuthTypeAnonymous {
		//we care about the bucket as a whole, not a particular resource
		url := *r.URL
		url.Path = "/" + bucket

		if s3Error := enforceBucketPolicy("s3:ListBucket", bucket, &url); s3Error != nil {
			return ErrAccessDenied
		}
	}
	return ErrNoSuchKey
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
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err, r.URL.Path)
			return
		}
	}
	version := r.URL.Query().Get("versionId")
	// Fetch object stat info.
	object, err := api.ObjectAPI.GetObjectInfo(bucketName, objectName, version, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to fetch object info.")
		if err == ErrNoSuchKey {
			err = errAllowableObjectNotFound(bucketName, r)
		}
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	if object.DeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
		WriteErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
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
				WriteErrorResponse(w, r, ErrInvalidRange, r.URL.Path)
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
			w.Header().Set("ETag", "\""+object.Etag+"\"")
		}
		if err == ContentNotModified { // write only header if is a 304
			WriteErrorResponseHeaders(w, r, err, r.URL.Path)
		} else {
			WriteErrorResponse(w, r, err, r.URL.Path)
		}
		return
	}

	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
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
			WriteErrorResponse(w, r, err, r.URL.Path)
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
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err, r.URL.Path)
			return
		}
	}

	version := r.URL.Query().Get("versionId")
	object, err := api.ObjectAPI.GetObjectInfo(bucketName, objectName, version, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to fetch object info.")
		if err == ErrNoSuchKey {
			err = errAllowableObjectNotFound(bucketName, r)
		}
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	if object.DeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
		WriteErrorResponse(w, r, ErrNoSuchKey, r.URL.Path)
		return
	}

	// Get request range.
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		if _, err = ParseRequestRange(rangeHeader, object.Size); err != nil {
			// Handle only ErrorInvalidRange
			// Ignore other parse error and treat it as regular Get request like Amazon S3.
			if err == ErrorInvalidRange {
				WriteErrorResponse(w, r, ErrInvalidRange, r.URL.Path)
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
			w.Header().Set("ETag", "\""+object.Etag+"\"")
		}
		if err == ContentNotModified { // write only header if is a 304
			WriteErrorResponseHeaders(w, r, err, r.URL.Path)
		} else {
			WriteErrorResponse(w, r, err, r.URL.Path)
		}
		return
	}

	_, err = parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
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
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err, r.URL.Path)
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
		WriteErrorResponse(w, r, ErrInvalidCopySource, r.URL.Path)
		return
	}

	splits = strings.SplitN(sourceObjectName, "?", 2)
	if len(splits) == 2 {
		sourceObjectName = splits[0]
		if !strings.HasPrefix(splits[1], "versionId=") {
			WriteErrorResponse(w, r, ErrInvalidCopySource, r.URL.Path)
			return
		}
		sourceVersion = strings.TrimPrefix(splits[1], "versionId=")
	}

	// X-Amz-Copy-Source should be URL-encoded
	sourceBucketName, err = url.QueryUnescape(sourceBucketName)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidCopySource, r.URL.Path)
		return
	}
	sourceObjectName, err = url.QueryUnescape(sourceObjectName)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidCopySource, r.URL.Path)
		return
	}

	helper.Debugln("sourceBucketName", sourceBucketName, "sourceObjectName", sourceObjectName,
		"sourceVersion", sourceVersion)

	sourceObject, err := api.ObjectAPI.GetObjectInfo(sourceBucketName, sourceObjectName,
		sourceVersion, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to fetch object info.")
		WriteErrorResponse(w, r, err, copySource)
		return
	}

	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	// Verify before x-amz-copy-source preconditions before continuing with CopyObject.
	if err = checkObjectPreconditions(w, r, sourceObject); err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	/// maximum Upload size for object in a single CopyObject operation.
	if isMaxObjectSize(sourceObject.Size) {
		WriteErrorResponse(w, r, ErrEntityTooLarge, copySource)
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
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	// Note that sourceObject and targetObject are pointers
	targetObject := sourceObject
	targetObject.ACL = targetAcl
	targetObject.BucketName = targetBucketName
	targetObject.Name = targetObjectName

	// Create the object.
	result, err := api.ObjectAPI.CopyObject(targetObject, pipeReader, credential, sseRequest)
	if err != nil {
		helper.ErrorIf(err, "Unable to create an object.")
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	response := GenerateCopyObjectResponse(result.Md5, result.LastModified)
	encodedSuccessResponse := EncodeResponse(response)
	// write headers
	SetCommonHeaders(w)
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
	// If the matching failed, it means that the X-Amz-Copy-Source was
	// wrong, fail right here.
	if _, ok := r.Header["X-Amz-Copy-Source"]; ok {
		WriteErrorResponse(w, r, ErrInvalidCopySource, r.URL.Path)
		return
	}
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	// Get Content-Md5 sent by client and verify if valid
	md5Bytes, err := checkValidMD5(r.Header.Get("Content-Md5"))
	if err != nil {
		helper.ErrorIf(err, "Unable to validate content-md5 format.")
		WriteErrorResponse(w, r, ErrInvalidDigest, r.URL.Path)
		return
	}
	// if Content-Length is unknown/missing, deny the request
	size := r.ContentLength
	if size == -1 && !contains(r.TransferEncoding, "chunked") {
		WriteErrorResponse(w, r, ErrMissingContentLength, r.URL.Path)
		return
	}
	// maximum Upload size for objects in a single operation
	if isMaxObjectSize(size) {
		WriteErrorResponse(w, r, ErrEntityTooLarge, r.URL.Path)
		return
	}

	// Save metadata.
	metadata := extractMetadataFromHeader(r.Header)
	metadata["md5Sum"] = hex.EncodeToString(md5Bytes)

	// Parse SSE related headers
	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	acl, err := getAclFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	credential, dataReader, err := signature.VerifyUpload(r)
	if err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	var result PutObjectResult
	result, err = api.ObjectAPI.PutObject(bucketName, objectName, credential, size, dataReader,
		metadata, acl, sseRequest)
	if err != nil {
		helper.ErrorIf(err, "Unable to create an object.")
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	if result.Md5 != "" {
		w.Header().Set("ETag", "\""+result.Md5+"\"")
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
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err, r.URL.Path)
			return
		}
	}

	version := r.URL.Query().Get("versionId")

	acl, err := getAclFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	err = api.ObjectAPI.SetObjectAcl(bucketName, objectName, version, acl, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to set ACL for object")
		WriteErrorResponse(w, r, err, r.URL.Path)
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
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err, r.URL.Path)
			return
		}
	}

	version := r.URL.Query().Get("versionId")

	object, err := api.ObjectAPI.GetObjectInfo(bucketName, objectName, version, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to fetch object info.")
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	if object.OwnerId != credential.UserId {
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	}

	w.Header().Set("X-Amz-Acl", object.ACL.CannedAcl)
	if version != "" {
		w.Header().Set("x-amz-version-id", version)
	}
	SetCommonHeaders(w)
	w.Write(nil)
}

/// Multipart objectAPIHandlers

// NewMultipartUploadHandler - New multipart upload
func (api ObjectAPIHandlers) NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	var credential iam.Credential
	var err error
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err, r.URL.Path)
			return
		}
	}

	acl, err := getAclFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	// Save metadata.
	metadata := extractMetadataFromHeader(r.Header)

	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	uploadID, err := api.ObjectAPI.NewMultipartUpload(credential, bucketName, objectName,
		metadata, acl, sseRequest)
	if err != nil {
		helper.ErrorIf(err, "Unable to initiate new multipart upload id.")
		WriteErrorResponse(w, r, err, r.URL.Path)
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
	// write headers
	SetCommonHeaders(w)
	// write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
}

// PutObjectPartHandler - Upload part
func (api ObjectAPIHandlers) PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectName := vars["object"]

	// get Content-Md5 sent by client and verify if valid
	md5Bytes, err := checkValidMD5(r.Header.Get("Content-Md5"))
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidDigest, r.URL.Path)
		return
	}

	/// if Content-Length is unknown/missing, throw away
	size := r.ContentLength
	if size == -1 {
		WriteErrorResponse(w, r, ErrMissingContentLength, r.URL.Path)
		return
	}

	/// maximum Upload size for multipart objects in a single operation
	if isMaxObjectSize(size) {
		WriteErrorResponse(w, r, ErrEntityTooLarge, r.URL.Path)
		return
	}

	uploadID := r.URL.Query().Get("uploadId")
	partIDString := r.URL.Query().Get("partNumber")

	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidPart, r.URL.Path)
		return
	}

	// check partID with maximum part ID for multipart objects
	if isMaxPartID(partID) {
		WriteErrorResponse(w, r, ErrInvalidMaxParts, r.URL.Path)
		return
	}

	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidMaxParts, r.URL.Path)
		return
	}

	credential, dataReader, err := signature.VerifyUpload(r)
	if err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	var result PutObjectPartResult
	incomingMD5 := hex.EncodeToString(md5Bytes)
	// No need to verify signature, anonymous request access is already allowed.
	result, err = api.ObjectAPI.PutObjectPart(bucketName, objectName, credential,
		uploadID, partID, size, dataReader, incomingMD5, sseRequest)
	if err != nil {
		helper.ErrorIf(err, "Unable to create object part.")
		// Verify if the underlying error is signature mismatch.
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	if result.ETag != "" {
		w.Header().Set("ETag", "\""+result.ETag+"\"")
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

	var credential iam.Credential
	var err error
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err, r.URL.Path)
			return
		}
	}

	targetUploadId := r.URL.Query().Get("uploadId")
	partIdString := r.URL.Query().Get("partNumber")

	targetPartId, err := strconv.Atoi(partIdString)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidPart, r.URL.Path)
		return
	}

	// check partID with maximum part ID for multipart objects
	if isMaxPartID(targetPartId) {
		WriteErrorResponse(w, r, ErrInvalidMaxParts, r.URL.Path)
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
		WriteErrorResponse(w, r, ErrInvalidCopySource, r.URL.Path)
		return
	}

	splits = strings.SplitN(sourceObjectName, "?", 2)
	if len(splits) == 2 {
		sourceObjectName = splits[0]
		if !strings.HasPrefix(splits[1], "versionId=") {
			WriteErrorResponse(w, r, ErrInvalidCopySource, r.URL.Path)
			return
		}
		sourceVersion = strings.TrimPrefix(splits[1], "versionId=")
	}

	// X-Amz-Copy-Source should be URL-encoded
	sourceBucketName, err = url.QueryUnescape(sourceBucketName)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidCopySource, r.URL.Path)
		return
	}
	sourceObjectName, err = url.QueryUnescape(sourceObjectName)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidCopySource, r.URL.Path)
		return
	}

	sourceObject, err := api.ObjectAPI.GetObjectInfo(sourceBucketName, sourceObjectName,
		sourceVersion, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to fetch object info.")
		WriteErrorResponse(w, r, err, copySource)
		return
	}

	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err, copySource)
		return
	}

	// Verify before x-amz-copy-source preconditions before continuing with CopyObject.
	if err = checkObjectPreconditions(w, r, sourceObject); err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
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
			WriteErrorResponse(w, r, ErrInvalidRange, r.URL.Path)
			return
		}
		readOffset = copySourceRange.OffsetBegin
		readLength = copySourceRange.GetLength()
		if isMaxObjectSize(copySourceRange.OffsetEnd - copySourceRange.OffsetBegin + 1) {
			WriteErrorResponse(w, r, ErrEntityTooLarge, copySource)
			return
		}
	}
	if isMaxObjectSize(readLength) {
		WriteErrorResponse(w, r, ErrEntityTooLarge, copySource)
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
		helper.ErrorIf(err, "Unable to create an object.")
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	response := GenerateCopyObjectPartResponse(result.Md5, result.LastModified)
	encodedSuccessResponse := EncodeResponse(response)
	// write headers
	SetCommonHeaders(w)
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
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err, r.URL.Path)
			return
		}
	}

	uploadId := r.URL.Query().Get("uploadId")
	if err := api.ObjectAPI.AbortMultipartUpload(credential, bucketName,
		objectName, uploadId); err != nil {

		helper.ErrorIf(err, "Unable to abort multipart upload.")
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}
	WriteSuccessNoContent(w)
}

// Send whitespace character, once every 5secs, until CompleteMultipartUpload is done.
// CompleteMultipartUpload method of the object layer indicates that it's done via doneCh
func sendWhiteSpaceChars(w http.ResponseWriter, doneCh <-chan struct{}) {
	for {
		select {
		case <-time.After(5 * time.Second):
			w.Write([]byte(" "))
		case <-doneCh:
			return
		}
	}
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
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err, r.URL.Path)
			return
		}
	}

	request, err := parseListObjectPartsQuery(r.URL.Query())
	if err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}
	listPartsInfo, err := api.ObjectAPI.ListObjectParts(credential, bucketName,
		objectName, request)
	if err != nil {
		helper.ErrorIf(err, "Unable to list uploaded parts.")
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}
	encodedSuccessResponse := EncodeResponse(listPartsInfo)
	// Write headers.
	SetCommonHeaders(w)
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
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err, r.URL.Path)
			return
		}
	}
	completeMultipartBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		helper.ErrorIf(err, "Unable to complete multipart upload.")
		WriteErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}
	complMultipartUpload := &meta.CompleteMultipartUpload{}
	if err = xml.Unmarshal(completeMultipartBytes, complMultipartUpload); err != nil {
		helper.ErrorIf(err, "Unable to parse complete multipart upload XML.")
		WriteErrorResponse(w, r, ErrMalformedXML, r.URL.Path)
		return
	}
	if len(complMultipartUpload.Parts) == 0 {
		WriteErrorResponse(w, r, ErrMalformedXML, r.URL.Path)
		return
	}
	if !sort.IsSorted(meta.CompletedParts(complMultipartUpload.Parts)) {
		WriteErrorResponse(w, r, ErrInvalidPartOrder, r.URL.Path)
		return
	}
	// Complete parts.
	var completeParts []meta.CompletePart
	for _, part := range complMultipartUpload.Parts {
		part.ETag = strings.TrimPrefix(part.ETag, "\"")
		part.ETag = strings.TrimSuffix(part.ETag, "\"")
		completeParts = append(completeParts, part)
	}
	// Complete multipart upload.
	// Send 200 OK
	SetCommonHeaders(w)
	w.WriteHeader(http.StatusOK)

	var result CompleteMultipartResult
	doneCh := make(chan struct{})
	// Signal that completeMultipartUpload is over via doneCh
	go func(doneCh chan<- struct{}) {
		result, err = api.ObjectAPI.CompleteMultipartUpload(credential, bucketName,
			objectName, uploadId, completeParts)
		doneCh <- struct{}{}
	}(doneCh)

	sendWhiteSpaceChars(w, doneCh)

	if err != nil {
		helper.ErrorIf(err, "Unable to complete multipart upload.")
		switch oErr := err.(type) {
		case meta.PartTooSmall:
			// Write part too small error.
			writePartSmallErrorResponse(w, r, oErr)
		default:
			// Handle all other generic issues.
			WriteErrorResponseNoHeader(w, r, err, r.URL.Path)
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
	w.Write(encodedSuccessResponse)
	w.(http.Flusher).Flush()
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
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypeSignedV4, signature.AuthTypePresignedV4,
		signature.AuthTypeSignedV2, signature.AuthTypePresignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err, r.URL.Path)
			return
		}
	}
	version := r.URL.Query().Get("versionId")
	/// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
	/// Ignore delete object errors, since we are supposed to reply
	/// only 204.
	result, err := api.ObjectAPI.DeleteObject(bucketName, objectName, version, credential)
	if err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}
	if result.DeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
	}
	if result.VersionId != "" {
		w.Header().Set("x-amz-version-id", result.VersionId)
	}
	WriteSuccessNoContent(w)
}
