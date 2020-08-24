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
	"encoding/json"
	"encoding/xml"
	"github.com/journeymidnight/yig/log"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	. "github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/policy"
	"github.com/journeymidnight/yig/backend"
	. "github.com/journeymidnight/yig/context"
	"github.com/journeymidnight/yig/crypto"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	. "github.com/journeymidnight/yig/meta/common"
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

func getStorageClassFromHeader(header http.Header) (StorageClass, error) {
	storageClassStr := header.Get("X-Amz-Storage-Class")

	if storageClassStr != "" {
		return MatchStorageClassIndex(storageClassStr)
	} else {
		// If you don't specify this header, Amazon S3 uses STANDARD
		return ObjectStorageClassStandard, nil
	}
}

// errAllowableNotFound - For an anon user, return 404 if have ListBucket, 403 otherwise
// this is in keeping with the permissions sections of the docs of both:
//   HEAD Object: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
//   GET Object: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
func (api ObjectAPIHandlers) errAllowableObjectNotFound(w http.ResponseWriter, r *http.Request, credential common.Credential) {

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
	ctx := GetRequestContext(r)
	if ctx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return
	}

	var p policy.Policy
	err := json.Unmarshal(ctx.BucketInfo.Policy, &p)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	if p.IsAllowed(policy.Args{
		Action:          policy.ListBucketAction,
		BucketName:      ctx.BucketName,
		ConditionValues: getConditionValues(r, ""),
		IsOwner:         false,
	}) == policy.PolicyAllow {
		err = ErrNoSuchKey
	} else {
		switch ctx.BucketInfo.ACL.CannedAcl {
		case "public-read", "public-read-write":
			err = ErrNoSuchKey
		case "authenticated-read":
			if credential.AccessKeyID != "" {
				err = ErrNoSuchKey
			} else {
				err = ErrAccessDenied
			}
		default:
			if ctx.BucketInfo.OwnerId == credential.ExternRootId {
				err = ErrNoSuchKey
			} else {
				err = ErrAccessDenied
			}
		}
	}
	var status int
	website := ctx.BucketInfo.Website
	apiErrorCode, ok := err.(ApiError)
	if ok {
		status = apiErrorCode.HttpStatusCode()
	} else {
		status = http.StatusInternalServerError
	}
	// match routing rules
	if len(website.RoutingRules) != 0 {
		for _, rule := range website.RoutingRules {
			// If the condition matches, handle redirect
			if rule.Match(ctx.ObjectName, strconv.Itoa(status)) {
				rule.DoRedirect(w, r, ctx.ObjectName)
				return
			}
		}
	}
	if api.ReturnWebsiteErrorDocument(w, r, status) {
		return
	}
	WriteErrorResponse(w, r, err)
}

type GetObjectResponseWriter struct {
	dataWritten bool
	w           http.ResponseWriter
	r           *http.Request
	object      *meta.Object
	hrange      *HttpRange
	statusCode  int
	version     string
}

func newGetObjectResponseWriter(w http.ResponseWriter, r *http.Request, object *meta.Object, hrange *HttpRange, statusCode int, version string) *GetObjectResponseWriter {
	return &GetObjectResponseWriter{false, w, r, object, hrange, statusCode, version}
}

func (o *GetObjectResponseWriter) Write(p []byte) (int, error) {
	if !o.dataWritten {
		if o.version != meta.NullVersion && o.version != "" {
			o.w.Header().Set("x-amz-version-id", o.version)
		}
		// Set any additional requested response headers.
		setGetRespHeaders(o.w, o.r.URL.Query())
		// Set headers on the first write.
		// Set standard object headers.
		SetObjectHeaders(o.w, o.object, o.hrange, o.statusCode)

		o.dataWritten = true
	}
	n, err := o.w.Write(p)
	if n > 0 {
		/*
			If the whole write or only part of write is successfull,
			n should be positive, so record this
		*/
		o.w.(*ResponseRecorder).size += int64(n)
	}
	return n, err
}

// GetObjectHandler - GET Object
// ----------
// This implementation of the GET operation retrieves object. To use GET,
// you must have READ access to the object.
func (api ObjectAPIHandlers) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpGetObject)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	var credential common.Credential
	var err error
	if api.HandledByWebsite(w, r) {
		return
	}

	if credential, err = checkRequestAuth(r, policy.GetObjectAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// Fetch object stat info.
	object, err := api.ObjectAPI.GetObjectInfoByCtx(reqCtx, credential)
	if err != nil {
		logger.Warn("Unable to fetch object info:", err)
		if err == ErrNoSuchKey {
			api.errAllowableObjectNotFound(w, r, credential)
			return
		}
		WriteErrorResponse(w, r, err)
		return
	}

	if object.DeleteMarker {
		if reqCtx.VersionId != "" {
			WriteErrorResponse(w, r, ErrMethodNotAllowed)
			return
		}
		SetObjectHeaders(w, object, nil, http.StatusNotFound)
		WriteErrorResponse(w, r, ErrNoSuchKey)
		return
	}
	if object.StorageClass == ObjectStorageClassGlacier {
		freezer, err := api.ObjectAPI.GetFreezer(reqCtx.BucketName, reqCtx.ObjectName, object.VersionId)
		if err != nil {
			if err == ErrNoSuchKey {
				logger.Warn("Unable to get glacier object with because of ErrNoSuchKey")
				WriteErrorResponse(w, r, ErrInvalidGlacierObject)
				return
			}
			logger.Error("Unable to get glacier object info err:", err)
			WriteErrorResponse(w, r, ErrInvalidRestoreInfo)
			return
		}
		if freezer.Status != ObjectHasRestored {
			logger.Warn("Unable to get glacier object because object has not restored:", freezer.Status)
			WriteErrorResponse(w, r, ErrInvalidGlacierObject)
			return
		}
		object.Parts = freezer.Parts
		object.Pool = freezer.Pool
		object.Location = freezer.Location
		object.ObjectId = freezer.ObjectId
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
			logger.Error("Invalid request range", err)
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
			WriteErrorResponseHeaders(w, r, err)
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

	// io.Writer type which keeps track if any data was written.
	writer := newGetObjectResponseWriter(w, r, object, hrange, http.StatusOK, object.VersionId)

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
		logger.Error("GetObject error:", err)
		if object.Type == meta.ObjectTypeAppendable && object.Pool == backend.SMALL_FILE_POOLNAME {
			info, err := api.ObjectAPI.GetObjectInfo(object.BucketName, object.ObjectId, object.VersionId, credential)
			if err != nil {
				logger.Error("Unable to fetch object info:", err)
				WriteErrorResponse(w, r, err)
				return
			}
			if info.Pool == backend.BIG_FILE_POOLNAME {
				logger.Warn("Found a object that been fetched during migrating :", err.Error())
				WriteErrorResponse(w, r, ErrObjectMovedPermanently)
				return
			}
		}
		if !writer.dataWritten {
			// Error response only if no data has been written to client yet. i.e if
			// partial data has already been written before an error
			// occurred then no point in setting StatusCode and
			// sending error XML.
			WriteErrorResponse(w, r, err)
			return
		}
		return
	}
	if !writer.dataWritten {
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
	//ResponseRecorder
	SetOperationName(w, OpHeadObject)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.GetObjectAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	reqVersion := reqCtx.VersionId
	object, err := api.ObjectAPI.GetObjectInfoByCtx(reqCtx, credential)
	if err != nil {
		logger.Warn("Unable to fetch object info:", err)
		if err == ErrNoSuchKey {
			api.errAllowableObjectNotFound(w, r, credential)
			return
		}
		WriteErrorResponse(w, r, err)
		return
	}

	if object.StorageClass == ObjectStorageClassGlacier {
		freezer, err := api.ObjectAPI.GetFreezerStatus(object.BucketName, object.Name, reqVersion)
		if err != nil && err != ErrNoSuchKey {
			logger.Warn("Unable to get restore object status", object.BucketName, object.Name, reqVersion,
				"error:", err)
			WriteErrorResponse(w, r, err)
			return
		}
		if freezer.Status == ObjectHasRestored {
			w.Header().Set("x-amz-restore", "ongoing-request='true'")
		} else {
			w.Header().Set("x-amz-restore", "ongoing-request='false'")
		}
	}

	if object.DeleteMarker {
		if reqCtx.VersionId != "" {
			WriteErrorResponse(w, r, ErrMethodNotAllowed)
			return
		}
		SetObjectHeaders(w, object, nil, http.StatusNotFound)
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
			logger.Error("Invalid request range:", err)
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
			WriteErrorResponseHeaders(w, r, err)
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
	// Set standard object headers.
	SetObjectHeaders(w, object, nil, http.StatusOK)
}

// CopyObjectHandler - Copy Object
// ----------
// This implementation of the PUT operation adds an object to a bucket
// while reading the object from another source.
func (api ObjectAPIHandlers) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {
	//ResponseRecorder
	SetOperationName(w, OpCopyObject)

	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	targetBucketName := reqCtx.BucketName
	targetObjectName := reqCtx.ObjectName
	targetBucket := reqCtx.BucketInfo

	if forbidOverwriteStr, ok := r.Header["X-Uos-Forbid-Overwrite"]; ok {
		forbidOverwrite, err := strconv.ParseBool(forbidOverwriteStr[0])
		if err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
		reqCtx.IsObjectForbidOverwrite = forbidOverwrite
	}

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.PutObjectAction); err != nil {
		WriteErrorResponse(w, r, err)
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
	if sourceBucketName == "" || sourceObjectName == "" {
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

	logger.Info("Copying object from", sourceBucketName, sourceObjectName,
		sourceVersion, "to", targetBucketName, targetObjectName)

	sourceBucket, err := api.ObjectAPI.GetBucket(sourceBucketName)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidCopySource)
	}

	credentialUpdate := credential
	err = checkSourceBucketAuth(r, policy.GetObjectAction, sourceBucket, sourceObjectName, &credentialUpdate)
	sourceObject, err := api.ObjectAPI.GetObjectInfo(sourceBucketName, sourceObjectName,
		sourceVersion, credentialUpdate)

	if err != nil {
		logger.Error("Unable to fetch object info:", err)
		WriteErrorResponseWithResource(w, r, err, copySource)
		return
	}

	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	if sseRequest.Type == "" {
		if configuration, ok := api.ObjectAPI.CheckBucketEncryption(reqCtx.BucketInfo); ok {
			if configuration.SSEAlgorithm == crypto.SSEAlgorithmAES256 {
				sseRequest.Type = crypto.S3.String()
			}
			//TODO:add kms
		}
	}
	if sseRequest.Type == "" {
		sseRequest.Type = sourceObject.SseType
	}

	// Verify before x-amz-copy-source preconditions before continuing with CopyObject.
	if err = checkObjectPreconditions(w, r, sourceObject); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	var targetStorageClass StorageClass
	targetStorageClass, err = getStorageClassFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	var isMetadataOnly, isTranStorageClassOnly bool

	// If the object metadata is modified or the object storage type is converted, the following conditions are met
	// For non-archive storage object modification metadata and type conversion, no copy operation is required
	// For objects that were originally archived and stored for metadata modification and storage type modification, only database metadata needs to be modified.
	if sourceBucketName == targetBucketName && sourceObjectName == targetObjectName {
		if sourceObject.StorageClass == ObjectStorageClassGlacier && targetStorageClass != ObjectStorageClassGlacier {
			WriteErrorResponse(w, r, ErrInvalidStorageClassConvert)
			return
		}
		if targetBucket.Versioning == BucketVersioningDisabled || (targetBucket.Versioning == BucketVersioningSuspended && sourceObject.VersionId == meta.NullVersion) {
			isMetadataOnly = true
		}
		if sourceObject.StorageClass != ObjectStorageClassGlacier && targetStorageClass == ObjectStorageClassGlacier {
			isMetadataOnly = false
		}
		if sourceObject.StorageClass != targetStorageClass {
			isTranStorageClassOnly = true
		}
	}

	truelySourceObject := sourceObject
	if sourceObject.StorageClass == ObjectStorageClassGlacier {
		// When only modifying object metadata, there is no need to unfreeze the object
		if !isMetadataOnly {
			freezer, err := api.ObjectAPI.GetFreezer(sourceBucketName, sourceObjectName, sourceObject.VersionId)
			if err != nil {
				if err == ErrNoSuchKey {
					logger.Error("Unable to get glacier object with no restore")
					WriteErrorResponse(w, r, ErrInvalidGlacierObject)
					return
				}
				logger.Error("Unable to get glacier object info err:", err)
				WriteErrorResponse(w, r, ErrInvalidRestoreInfo)
				return
			}
			if freezer.Status != ObjectHasRestored {
				logger.Error("Unable to get glacier object with no restore")
				WriteErrorResponse(w, r, ErrInvalidGlacierObject)
				return
			}
			sourceObject.Parts = freezer.Parts
			sourceObject.PartsIndex = freezer.PartsIndex
			sourceObject.Pool = freezer.Pool
			sourceObject.Location = freezer.Location
			sourceObject.ObjectId = freezer.ObjectId
		}
	}

	// maximum Upload size for object in a single CopyObject operation.
	if isMaxObjectSize(sourceObject.Size) {
		WriteErrorResponseWithResource(w, r, ErrEntityTooLarge, copySource)
		return
	}

	targetACL, err := getAclFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// Note that sourceObject and targetObject are pointers
	targetObject := &meta.Object{}
	targetObject.ACL = targetACL
	targetObject.OwnerId = sourceObject.OwnerId
	targetObject.BucketName = targetBucketName
	targetObject.Name = targetObjectName
	targetObject.Size = sourceObject.Size
	targetObject.Etag = sourceObject.Etag
	targetObject.Parts = sourceObject.Parts
	targetObject.Type = sourceObject.Type
	targetObject.ObjectId = sourceObject.ObjectId
	targetObject.Pool = sourceObject.Pool
	targetObject.Location = sourceObject.Location
	targetObject.StorageClass = targetStorageClass
	targetObject.CreateTime = uint64(time.Now().UnixNano())

	directive := r.Header.Get("X-Amz-Metadata-Directive")
	if directive == "COPY" || directive == "" {
		targetObject.CustomAttributes = sourceObject.CustomAttributes
		targetObject.ContentType = sourceObject.ContentType
	} else if directive == "REPLACE" {
		newMetadata := extractMetadataFromHeader(r.Header)
		if c, ok := newMetadata["content-type"]; ok {
			targetObject.ContentType = c
		} else {
			targetObject.ContentType = sourceObject.ContentType
		}
		targetObject.CustomAttributes = newMetadata
	} else {
		WriteErrorResponse(w, r, ErrInvalidCopyRequest)
		return
	}

	pipeReader, pipeWriter := io.Pipe()
	if !isMetadataOnly {
		go func() {
			startOffset := int64(0) // Read the whole file.
			// Get the object.
			err = api.ObjectAPI.GetObject(sourceObject, startOffset, sourceObject.Size,
				pipeWriter, sseRequest)
			if err != nil {
				logger.Error("Unable to read an object:", err)
				pipeWriter.CloseWithError(err)
				return
			}
			pipeWriter.Close()
		}()
	}

	// Create the object.
	result, err := api.ObjectAPI.CopyObject(reqCtx, targetObject, truelySourceObject, pipeReader, credential, sseRequest, isMetadataOnly, isTranStorageClassOnly)
	if err != nil {
		logger.Error("CopyObject failed:", err)
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

	for s, v := range result.DeltaInfo {
		SetDeltaSize(w, s, v)
	}
	// write success response.
	WriteSuccessResponse(w, r, encodedSuccessResponse)
	// Explicitly close the reader, to avoid fd leaks.
	pipeReader.Close()
}

// RenameObjectHandler - Rename Object
// ----------
// Do not support bucket to enable multiVersion renaming;
// Folder renaming operation is not supported.
func (api ObjectAPIHandlers) RenameObjectHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpRenameObject)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	bucketName := reqCtx.BucketName
	targetObjectName := reqCtx.ObjectName

	// Determine if the renamed object is legal
	if hasSuffix(reqCtx.ObjectName, "/") || reqCtx.ObjectName == "" {
		WriteErrorResponse(w, r, ErrInvalidRenameTarget)
		return
	}

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.PutObjectAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	if reqCtx.ObjectInfo != nil {
		WriteErrorResponse(w, r, ErrInvalidRenameTarget)
		return
	}

	sourceObjectName := r.Header.Get("X-Amz-Rename-Source-Key")

	if sourceObjectName == reqCtx.ObjectName {
		WriteErrorResponse(w, r, ErrInvalidRenameTarget)
		return
	}

	// X-Amz-Copy-Source should be URL-encoded
	sourceObjectName, err = url.QueryUnescape(sourceObjectName)
	if err != nil {
		WriteErrorResponse(w, r, ErrInvalidRenameSourceKey)
		return
	}

	// Determine if the renamed object is a folder
	if hasSuffix(sourceObjectName, "/") {
		WriteErrorResponse(w, r, ErrInvalidRenameSourceKey)
		return
	}
	logger.Info("Renaming object from", bucketName, sourceObjectName,
		"to", targetObjectName)

	//TODO: Supplement Object MultiVersion Judge.
	bucket := reqCtx.BucketInfo
	if bucket.Versioning != BucketVersioningDisabled {
		WriteErrorResponse(w, r, ErrNotSupportBucketEnabledVersion)
		return
	}
	logger.Info("Bucket Multi-version is:", bucket.Versioning)

	var sourceVersion string
	sourceObject, err := api.ObjectAPI.GetObjectInfo(reqCtx.BucketName, sourceObjectName,
		sourceVersion, credential)
	if err != nil {
		WriteErrorResponseWithResource(w, r, err, sourceObjectName)
		return
	}

	targetObject := sourceObject
	targetObject.Name = reqCtx.ObjectName
	result, err := api.ObjectAPI.RenameObject(reqCtx, targetObject, sourceObjectName, credential)
	if err != nil {
		logger.Warn("Unable to update object meta for", targetObject.Name,
			"error:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	response := GenerateRenameObjectResponse(result.LastModified)
	encodedSuccessResponse := EncodeResponse(response)

	// write success response.
	WriteSuccessResponse(w, r, encodedSuccessResponse)
}

// PutObjectHandler - PUT Object
// ----------
// This implementation of the PUT operation adds an object to a bucket.
func (api ObjectAPIHandlers) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpPutObject)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	// If the matching failed, it means that the X-Amz-Copy-Source was
	// wrong, fail right here.
	if _, ok := r.Header["X-Amz-Copy-Source"]; ok {
		WriteErrorResponse(w, r, ErrInvalidCopySource)
		return
	}

	var err error
	if !isValidObjectName(reqCtx.ObjectName) {
		WriteErrorResponse(w, r, ErrInvalidObjectName)
		return
	}

	if forbidOverwriteStr, ok := r.Header["X-Uos-Forbid-Overwrite"]; ok {
		forbidOverwrite, err := strconv.ParseBool(forbidOverwriteStr[0])
		if err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
		reqCtx.IsObjectForbidOverwrite = forbidOverwrite
	}
	// if Content-Length is unknown/missing, deny the request
	size := r.ContentLength
	if reqCtx.AuthType == signature.AuthTypeStreamingSigned {
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

	storageClass, err := getStorageClassFromHeader(r.Header)
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
			logger.Info("Content Md5 is null")
			WriteErrorResponse(w, r, ErrInvalidDigest)
			return
		}
		md5Bytes, err := checkValidMD5(r.Header.Get("Content-Md5"))
		if err != nil {
			logger.Info("Content Md5 is invalid")
			WriteErrorResponse(w, r, ErrInvalidDigest)
			return
		} else {
			metadata["md5Sum"] = hex.EncodeToString(md5Bytes)
		}
	}

	if reqCtx.AuthType == signature.AuthTypeStreamingSigned {
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
	// Support SSE-S3 and SSE-C now
	var sseRequest SseRequest

	if hasServerSideEncryptionHeader(r.Header) && !hasSuffix(reqCtx.ObjectName, "/") { // handle SSE requests
		sseRequest, err = parseSseHeader(r.Header)
		if err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	} else if configuration, ok := api.ObjectAPI.CheckBucketEncryption(reqCtx.BucketInfo); ok {
		if configuration.SSEAlgorithm == crypto.SSEAlgorithmAES256 {
			sseRequest.Type = crypto.S3.String()
		}
		//TODO:add kms
	}

	acl, err := getAclFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	credential, dataReadCloser, err := signature.VerifyUpload(r)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// do bucket policy check first
	isAllow, err := IsBucketPolicyAllowed(&credential, reqCtx.BucketInfo, r, policy.PutObjectAction, reqCtx.ObjectName)
	helper.Logger.Info("checkRequestAuth1:", isAllow, err)
	if err == nil && isAllow == false {
		//then do ram policy check if the request is from a sub user of who own this bucket
		if credential.ExternRootId == reqCtx.BucketInfo.OwnerId {
			isAllow, err = IsRamPolicyAllowed(credential.Policy, r, policy.PutObjectAction)
			helper.Logger.Info("checkRequestAuth2:", isAllow, err)
		}
	}
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	credential.AllowOtherUserAccess = isAllow

	var result PutObjectResult
	result, err = api.ObjectAPI.PutObject(reqCtx, credential, size, dataReadCloser,
		metadata, acl, sseRequest, storageClass)
	if err != nil {
		logger.Error("Unable to create object", reqCtx.ObjectName, "error:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	isCallback, callbackMessage, err := GetCallbackFromHeader(r.Header)
	if err != nil {
		logger.Warn("Unable to get callback info with PutObject request:", reqCtx.ObjectName, "error:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	if isCallback {
		callbackMagicInfos := CallBackMagicInfos{
			BucketName: reqCtx.BucketName,
			FileName:   reqCtx.ObjectName,
			VersionId:  reqCtx.VersionId,
			Etag:       result.Md5,
			ObjectSize: result.ObjectSize,
			MimeType:   metadata["Content-Type"],
			CreateTime: uint64(result.LastModified.UnixNano()),
		}
		callbackMessage.Credential = credential
		resultCallback, err := api.CallbackProcess(callbackMagicInfos, callbackMessage, logger, credential)
		if err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
		w.Header().Add("x-uos-callback-result", resultCallback)
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
	for sc, v := range result.DeltaInfo {
		SetDeltaSize(w, sc, v)
	}
	WriteSuccessResponse(w, r, nil)
}

// AppendObjectHandler - Append Object
// ----------
// This implementation of the POST operation append an object in a bucket.
func (api ObjectAPIHandlers) AppendObjectHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpAppendObject)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	bucketName := reqCtx.BucketName
	objectName := reqCtx.ObjectName

	logger.Info("Appending object:", bucketName, objectName)

	var authType = signature.GetRequestAuthType(r)
	var err error

	if !isValidObjectName(objectName) {
		WriteErrorResponse(w, r, ErrInvalidObjectName)
		return
	}

	pos := r.URL.Query().Get("position")
	// Parse SSE related headers
	// Support SSE-S3 and SSE-C now
	var sseRequest SseRequest
	var position uint64
	var acl Acl

	if position, err = checkPosition(pos); err != nil {
		WriteErrorResponse(w, r, ErrInvalidPosition)
		return
	}

	// if Content-Length is unknown/missing, deny the request
	size := r.ContentLength
	if authType == signature.AuthTypeStreamingSigned {
		if sizeStr := r.Header.Get("X-Amz-Decoded-Content-Length"); sizeStr != "" {
			size, err = strconv.ParseInt(sizeStr, 10, 64)
			if err != nil {
				WriteErrorResponse(w, r, err)
				return
			}
		} else {
			WriteErrorResponse(w, r, ErrMissingContentLength)
			return
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

	storageClass, err := getStorageClassFromHeader(r.Header)
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
			logger.Warn("Content Md5 is null")
			WriteErrorResponse(w, r, ErrInvalidDigest)
			return
		}
		md5Bytes, err := checkValidMD5(r.Header.Get("Content-Md5"))
		if err != nil {
			logger.Warn("Content Md5 is invalid")
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
	credential, dataReadCloser, err := signature.VerifyUpload(r)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// do bucket policy check first
	isAllow, err := IsBucketPolicyAllowed(&credential, reqCtx.BucketInfo, r, policy.PutObjectAction, reqCtx.ObjectName)
	helper.Logger.Info("checkRequestAuth1:", isAllow, err)
	if err == nil && isAllow == false {
		//then do ram policy check if the request is from a sub user of who own this bucket
		if credential.ExternRootId == reqCtx.BucketInfo.OwnerId {
			isAllow, err = IsRamPolicyAllowed(credential.Policy, r, policy.PutObjectAction)
			helper.Logger.Info("checkRequestAuth2:", isAllow, err)
		}
	}
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	credential.AllowOtherUserAccess = isAllow

	// Check whether the object is exist or not
	// Check whether the bucket is owned by the specified user
	objInfo, err := api.ObjectAPI.GetObjectInfoByCtx(reqCtx, credential)
	if err != nil && err != ErrNoSuchKey {
		WriteErrorResponse(w, r, err)
		return
	}

	if objInfo != nil && objInfo.Type != meta.ObjectTypeAppendable {
		WriteErrorResponse(w, r, ErrObjectNotAppendable)
		return
	}

	if objInfo != nil && objInfo.Size != int64(position) {
		logger.Info("Current Size:", objInfo.Size, "Position:", position)
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
	result, err = api.ObjectAPI.AppendObject(reqCtx, credential, position, size, dataReadCloser,
		metadata, acl, sseRequest, storageClass, objInfo)
	if err != nil {
		logger.Error("Unable to append object", objectName, "error:", err)
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
	} {
		if header := r.Header.Get(headerName); header != "" {
			w.Header().Set(headerName, header)
		}
	}

	if reqCtx.ObjectInfo == nil {
		SetDeltaSize(w, storageClass, size)
	} else {
		SetDeltaSize(w, reqCtx.ObjectInfo.StorageClass, size)
	}

	// Set next position
	w.Header().Set("X-Amz-Next-Append-Position", strconv.FormatInt(result.NextPosition, 10))

	WriteSuccessResponse(w, r, nil)
}

func (api ObjectAPIHandlers) PutObjectMeta(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpPutObjectMeta)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	bucketName := reqCtx.BucketName
	objectName := reqCtx.ObjectName

	logger.Info("Put object meta:", bucketName, objectName)

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.PutObjectAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	if r.ContentLength <= 0 {
		WriteErrorResponse(w, r, ErrMissingContentLength)
		return
	}

	if r.ContentLength > MaxObjectMetaConfigurationSize {
		WriteErrorResponse(w, r, ErrEntityTooLarge)
		return
	}

	metaData, err := ParseMetaConfig(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	object := reqCtx.ObjectInfo
	object.CustomAttributes = metaData.Data

	err = api.ObjectAPI.PutObjectMeta(reqCtx.BucketInfo, object, credential)
	if err != nil {
		logger.Warn("Unable to update object meta for", object.Name,
			"error:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	WriteSuccessResponse(w, r, nil)
}

func (api ObjectAPIHandlers) RestoreObjectHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpRestoreObject)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	var credential common.Credential
	var err error
	if api.HandledByWebsite(w, r) {
		return
	}

	if credential, err = checkRequestAuth(r, policy.GetObjectAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// Fetch object stat info.
	object, err := api.ObjectAPI.GetObjectInfoByCtx(reqCtx, credential)
	if err != nil {
		logger.Error("Unable to fetch object info:", err)
		if err == ErrNoSuchKey {
			api.errAllowableObjectNotFound(w, r, credential)
			return
		}
		WriteErrorResponse(w, r, err)
		return
	}

	if object.StorageClass != ObjectStorageClassGlacier {
		WriteErrorResponse(w, r, ErrInvalidStorageClass)
		return
	}

	if object.DeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
		WriteErrorResponse(w, r, ErrNoSuchKey)
		return
	}

	info, err := GetRestoreInfo(r)
	if err != nil {
		logger.Error("Unable to get freezer info:", err)
		WriteErrorResponse(w, r, ErrInvalidRestoreInfo)
		return
	}

	freezer, err := api.ObjectAPI.GetFreezerStatus(object.BucketName, object.Name, object.VersionId)
	if err != nil && err != ErrNoSuchKey {
		logger.Error("Unable to get restore object status", object.BucketName, object.Name,
			"error:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	if err == ErrNoSuchKey || freezer.Name == "" {
		status, err := MatchStatusIndex("READY")
		if err != nil {
			logger.Warn("Unable to get freezer status:", err)
			WriteErrorResponse(w, r, ErrInvalidRestoreInfo)
			return
		}

		lifeTime := info.Days
		if lifeTime < 1 || lifeTime > 30 {
			logger.Warn("The user has set the wrong defrost time")
			WriteErrorResponse(w, r, ErrInvalidRestoreDate)
			return
		}

		targetFreezer := &meta.Freezer{}
		targetFreezer.BucketName = object.BucketName
		targetFreezer.Name = object.Name
		targetFreezer.Status = status
		targetFreezer.LifeTime = lifeTime
		targetFreezer.Type = object.Type
		targetFreezer.CreateTime = object.CreateTime
		targetFreezer.VersionId = object.VersionId
		if helper.CONFIG.FakeRestore {
			targetFreezer.Pool = object.Pool
			targetFreezer.Location = object.Location
			targetFreezer.ObjectId = object.ObjectId
			targetFreezer.Parts = object.Parts
			targetFreezer.PartsIndex = object.PartsIndex
			err = api.ObjectAPI.RestoreObject(targetFreezer)
		} else {
			err = api.ObjectAPI.CreateFreezer(targetFreezer)
		}
		if err != nil {
			logger.Error("Unable to create freezer:", err)
			WriteErrorResponse(w, r, ErrCreateRestoreObject)
			return
		}
		logger.Info("Submit thaw request successfully")

		// ResponseRecorder
		w.WriteHeader(http.StatusAccepted)
		WriteSuccessResponseWithStatus(w, nil, http.StatusAccepted)
		return
	}

	if freezer.Status == ObjectHasRestored {
		err = api.ObjectAPI.UpdateFreezerDate(freezer, info.Days, true)
		if err != nil {
			if err == ErrInvalidRestoreDate {
				logger.Warn("The user has set the wrong defrost time")
				WriteErrorResponse(w, r, err)
				return
			}
			logger.Error("Unable to Update freezer date:", err)
			WriteErrorResponse(w, r, ErrInvalidRestoreInfo)
			return
		}

		// ResponseRecorder
		WriteSuccessResponse(w, r, nil)
		return
	} else {
		if freezer.LifeTime != info.Days {
			err = api.ObjectAPI.UpdateFreezerDate(freezer, info.Days, false)
			if err != nil {
				if err == ErrInvalidRestoreDate {
					logger.Warn("The user has set the wrong defrost time")
					WriteErrorResponse(w, r, err)
					return
				}
				logger.Error("Unable to Update freezer date:", err)
				WriteErrorResponse(w, r, ErrInvalidRestoreInfo)
				return
			}
		}

		WriteSuccessResponseWithStatus(w, nil, http.StatusAccepted)
		return
	}
}

func (api ObjectAPIHandlers) PutObjectAclHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpPutObjectAcl)
	reqCtx := GetRequestContext(r)
	logger := ContextLogger(r)
	objectName := reqCtx.ObjectName

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.PutObjectAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	var acl Acl
	if _, ok := r.Header["X-Amz-Acl"]; ok {
		acl, err = getAclFromHeader(r.Header)
		if err != nil {
			WriteErrorResponse(w, r, ErrInvalidAcl)
			return
		}
	} else {
		aclBuffer, err := ioutil.ReadAll(io.LimitReader(r.Body, 1024))
		logger.Info("ACL body:", string(aclBuffer))
		if err != nil {
			logger.Error("Unable to read ACLs body:", err)
			WriteErrorResponse(w, r, ErrInvalidAcl)
			return
		}
		err = xml.Unmarshal(aclBuffer, &acl.Policy)
		if err != nil {
			logger.Error("Unable to Unmarshal XML for ACL:", err)
			WriteErrorResponse(w, r, ErrInternalError)
			return
		}
	}

	err = api.ObjectAPI.SetObjectAcl(reqCtx, acl, credential)
	if err != nil {
		logger.Error("Unable to set ACL for object", objectName,
			"error:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	if reqCtx.VersionId != "" {
		w.Header().Set("x-amz-version-id", reqCtx.VersionId)
	}

	WriteSuccessResponse(w, r, nil)
}

func (api ObjectAPIHandlers) GetObjectAclHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpGetObjectAcl)
	reqCtx := GetRequestContext(r)
	logger := ContextLogger(r)
	objectName := reqCtx.ObjectName

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.GetObjectAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	acl, err := api.ObjectAPI.GetObjectAcl(reqCtx, credential)
	if err != nil {
		logger.Warn("Unable to fetch object acl:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	aclBuffer, err := xmlFormat(acl)
	if err != nil {
		logger.Error("Failed to marshal ACL XML for object", objectName,
			"error:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	if reqCtx.ObjectInfo.VersionId != meta.NullVersion {
		w.Header().Set("x-amz-version-id", reqCtx.ObjectInfo.VersionId)
	}

	setXmlHeader(w)

	WriteSuccessResponse(w, r, aclBuffer)
}

// Multipart objectAPIHandlers

// NewMultipartUploadHandler - New multipart upload
func (api ObjectAPIHandlers) NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpNewMultipartUpload)
	reqCtx := GetRequestContext(r)
	logger := ContextLogger(r)
	bucketName := reqCtx.BucketName
	objectName := reqCtx.ObjectName

	if !isValidObjectName(objectName) {
		WriteErrorResponse(w, r, ErrInvalidObjectName)
		return
	}

	if forbidOverwriteStr, ok := r.Header["X-Uos-Forbid-Overwrite"]; ok {
		forbidOverwrite, err := strconv.ParseBool(forbidOverwriteStr[0])
		if err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
		reqCtx.IsObjectForbidOverwrite = forbidOverwrite
	}

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.PutObjectAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
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
	} else if configuration, ok := api.ObjectAPI.CheckBucketEncryption(reqCtx.BucketInfo); ok {
		if configuration.SSEAlgorithm == crypto.SSEAlgorithmAES256 {
			sseRequest.Type = crypto.S3.String()
		}
		//TODO:add kms
	}

	storageClass, err := getStorageClassFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	uploadID, err := api.ObjectAPI.NewMultipartUpload(reqCtx, credential, metadata, acl, sseRequest, storageClass)
	if err != nil {
		logger.Error("Unable to initiate new multipart upload id:", err)
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
	WriteSuccessResponse(w, r, encodedSuccessResponse)
}

// PutObjectPartHandler - Upload part
func (api ObjectAPIHandlers) PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpPutObjectPart)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	authType := reqCtx.AuthType

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

	credential, dataReadCloser, err := signature.VerifyUpload(r)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// do bucket policy check first
	isAllow, err := IsBucketPolicyAllowed(&credential, reqCtx.BucketInfo, r, policy.PutObjectAction, reqCtx.ObjectName)
	helper.Logger.Info("checkRequestAuth1:", isAllow, err)
	if err == nil && isAllow == false {
		//then do ram policy check if the request is from a sub user of who own this bucket
		if credential.ExternRootId == reqCtx.BucketInfo.OwnerId {
			isAllow, err = IsRamPolicyAllowed(credential.Policy, r, policy.PutObjectAction)
			helper.Logger.Info("checkRequestAuth2:", isAllow, err)
		}
	}
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	credential.AllowOtherUserAccess = isAllow

	var result PutObjectPartResult
	// No need to verify signature, anonymous request access is already allowed.
	result, err = api.ObjectAPI.PutObjectPart(reqCtx, credential,
		uploadID, partID, size, dataReadCloser, incomingMd5, sseRequest)
	if err != nil {
		logger.Error("Unable to create object part for", reqCtx.ObjectName, "error:", err)
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

	SetDeltaSize(w, result.DeltaSize.StorageClass, result.DeltaSize.Delta)
	WriteSuccessResponse(w, r, nil)
}

// Upload part - copy
func (api ObjectAPIHandlers) CopyObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	//ResponseRecorder
	SetOperationName(w, OpCopyObjectPart)
	reqCtx := GetRequestContext(r)
	logger := ContextLogger(r)
	targetBucketName := reqCtx.BucketName
	targetObjectName := reqCtx.ObjectName

	if !isValidObjectName(targetObjectName) {
		WriteErrorResponse(w, r, ErrInvalidObjectName)
		return
	}

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.PutObjectAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
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
	if sourceVersion == "" {
		sourceVersion = "0"
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
		logger.Error("Unable to fetch object info:", err)
		WriteErrorResponseWithResource(w, r, err, copySource)
		return
	}

	if sourceObject.StorageClass == ObjectStorageClassGlacier {
		freezer, err := api.ObjectAPI.GetFreezer(sourceBucketName, sourceObjectName, sourceVersion)
		if err != nil {
			if err == ErrNoSuchKey {
				logger.Warn("Unable to get glacier object with no restore")
				WriteErrorResponse(w, r, ErrInvalidGlacierObject)
				return
			}
			logger.Error("Unable to get glacier object info err:", err)
			WriteErrorResponse(w, r, ErrInvalidRestoreInfo)
			return
		}
		if freezer.Status != ObjectHasRestored {
			logger.Warn("Unable to get glacier object with no restore")
			err = ErrInvalidGlacierObject
			return
		}
		sourceObject.Size = freezer.Size
		sourceObject.Parts = freezer.Parts
		sourceObject.Pool = freezer.Pool
		sourceObject.Location = freezer.Location
		sourceObject.ObjectId = freezer.ObjectId
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
			logger.Warn("Invalid request range", err)
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
			logger.Error("Unable to read an object:", err)
			pipeWriter.CloseWithError(err)
			return
		}
		pipeWriter.Close()
	}()

	// Create the object.
	result, err := api.ObjectAPI.CopyObjectPart(targetBucketName, targetObjectName, targetUploadId,
		targetPartId, readLength, pipeReader, credential, sseRequest)
	if err != nil {
		logger.Error("Unable to copy object part from", sourceObjectName,
			"to", targetObjectName, "error:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	response := GenerateCopyObjectPartResponse(result.ETag, result.LastModified)
	encodedSuccessResponse := EncodeResponse(response)
	// write headers
	if result.ETag != "" {
		w.Header()["ETag"] = []string{"\"" + result.ETag + "\""}
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
	SetDeltaSize(w, result.DeltaSize.StorageClass, result.DeltaSize.Delta)
	// write success response.
	WriteSuccessResponse(w, r, encodedSuccessResponse)
}

// AbortMultipartUploadHandler - Abort multipart upload
func (api ObjectAPIHandlers) AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpAbortMultipartUpload)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error

	if credential, err = checkRequestAuth(r, policy.AbortMultipartUploadAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	uploadId := r.URL.Query().Get("uploadId")
	delta, err := api.ObjectAPI.AbortMultipartUpload(reqCtx, credential, uploadId)
	if err != nil {
		logger.Error("Unable to abort multipart upload:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	SetDeltaSize(w, delta.StorageClass, delta.Delta)
	WriteSuccessNoContent(w)
}

// ListObjectPartsHandler - List object parts
func (api ObjectAPIHandlers) ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpListObjectParts)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	bucketName := reqCtx.BucketName
	objectName := reqCtx.ObjectName

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.ListMultipartUploadPartsAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	request, err := parseListObjectPartsQuery(r.URL.Query())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	listPartsInfo, err := api.ObjectAPI.ListObjectParts(credential, bucketName,
		objectName, request)
	if err != nil {
		logger.Error("Unable to list uploaded parts:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	encodedSuccessResponse := EncodeResponse(listPartsInfo)
	// Write success response.
	WriteSuccessResponse(w, r, encodedSuccessResponse)
}

// CompleteMultipartUploadHandler - Complete multipart upload
func (api ObjectAPIHandlers) CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpCompleteMultipartUpload)
	reqCtx := GetRequestContext(r)
	logger := ContextLogger(r)

	// Get upload id.
	uploadId := r.URL.Query().Get("uploadId")

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.PutObjectAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	completeMultipartBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logger.Error(
			"Unable to complete multipart upload when read request body:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
	complMultipartUpload := &meta.CompleteMultipartUpload{}
	if err = xml.Unmarshal(completeMultipartBytes, complMultipartUpload); err != nil {
		logger.Error("Unable to parse complete multipart upload XML. data:",
			string(completeMultipartBytes), "error:", err)
		WriteErrorResponse(w, r, ErrMalformedXML)
		return
	}
	if len(complMultipartUpload.Parts) == 0 {
		logger.Error("Unable to complete multipart upload: " +
			"len(complMultipartUpload.Parts) == 0")
		WriteErrorResponse(w, r, ErrMalformedXML)
		return
	}
	if !sort.IsSorted(meta.CompletedParts(complMultipartUpload.Parts)) {
		logger.Error("Unable to complete multipart upload. data:",
			complMultipartUpload.Parts, "parts not sorted")
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
	result, err = api.ObjectAPI.CompleteMultipartUpload(reqCtx, credential, uploadId, completeParts)

	if err != nil {
		logger.Error("Unable to complete multipart upload:", err)
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

	isCallback, callbackMessage, err := GetCallbackFromHeader(r.Header)
	if err != nil {
		logger.Warn("Unable to get callback info with Complete multipart upload request:", reqCtx.ObjectName, "error:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	if isCallback {
		callbackMagicInfos := CallBackMagicInfos{
			BucketName: reqCtx.BucketName,
			FileName:   reqCtx.ObjectName,
			VersionId:  reqCtx.VersionId,
			Etag:       result.ETag,
			ObjectSize: result.ObjectSize,
			MimeType:   result.ContentType,
			CreateTime: result.CreateTime,
		}
		callbackMessage.Credential = credential
		resultCallback, err := api.CallbackProcess(callbackMagicInfos, callbackMessage, logger, credential)
		if err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
		w.Header().Add("x-uos-callback-result", resultCallback)
	}

	// Get object location.
	location := GetLocation(r)
	// Generate complete multipart response.
	response := GenerateCompleteMultpartUploadResponse(reqCtx.BucketName, reqCtx.ObjectName, location, result.ETag)
	encodedSuccessResponse, err := xmlFormat(response)
	if err != nil {
		logger.Error("Unable to parse CompleteMultipartUpload response:", err)
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

	if reqCtx.ObjectInfo != nil {
		SetDeltaSize(w, reqCtx.ObjectInfo.StorageClass, -reqCtx.ObjectInfo.Size)
	}

	setXmlHeader(w)

	// write success response.
	WriteSuccessResponse(w, r, encodedSuccessResponse)
}

// Delete objectAPIHandlers

// DeleteObjectHandler - delete an object
func (api ObjectAPIHandlers) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpDeleteObject)
	reqCtx := GetRequestContext(r)
	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.DeleteObjectAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
	// Ignore delete object errors, since we are supposed to reply only 204.
	result, err := api.ObjectAPI.DeleteObject(reqCtx, credential)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	if result.DeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
	}
	if result.VersionId != "" && result.VersionId != meta.NullVersion {
		w.Header().Set("x-amz-version-id", result.VersionId)
	}

	SetDeltaSize(w, result.DeltaSize.StorageClass, result.DeltaSize.Delta)

	var unexpiredInfo []UnexpiredTriple
	if ok, delta := reqCtx.ObjectInfo.IsUnexpired(); ok {
		unexpiredInfo = append(unexpiredInfo, UnexpiredTriple{
			StorageClass: reqCtx.ObjectInfo.StorageClass,
			Size:         CorrectDeltaSize(reqCtx.ObjectInfo.StorageClass, reqCtx.ObjectInfo.Size),
			SurvivalTime: delta,
		})
	}
	SetUnexpiredInfo(w, unexpiredInfo)
	WriteSuccessNoContent(w)
}

// PostPolicyBucketHandler - POST policy upload
// ----------
// This implementation of the POST operation handles object creation with a specified
// signature policy in multipart/form-data

var ValidSuccessActionStatus = []string{"200", "201", "204"}

func (api ObjectAPIHandlers) PostObjectHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpPostObject)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	var err error
	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary files.

	fileBody, formValues := reqCtx.Body, reqCtx.FormValues

	bucketName, objectName := reqCtx.BucketName, reqCtx.ObjectName
	formValues["Bucket"] = bucketName
	if !isValidObjectName(objectName) {
		WriteErrorResponse(w, r, ErrInvalidObjectName)
		return
	}
	if forbidOverwriteStr, ok := formValues["X-Uos-Forbid-Overwrite"]; ok {
		forbidOverwrite, err := strconv.ParseBool(forbidOverwriteStr)
		if err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
		reqCtx.IsObjectForbidOverwrite = forbidOverwrite
	}
	bucket := reqCtx.BucketInfo
	logger.Info("PostObjectHandler formValues", formValues)

	var credential common.Credential
	postPolicyType := signature.GetPostPolicyType(formValues)
	logger.Info("type", postPolicyType)
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

	if err = signature.CheckPostPolicy(formValues); err != nil {
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
	acl.CannedAcl = headerfiedFormValues.Get("Acl")
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
	if sseRequest.Type == "" {
		if configuration, ok := api.ObjectAPI.CheckBucketEncryption(reqCtx.BucketInfo); ok {
			if configuration.SSEAlgorithm == crypto.SSEAlgorithmAES256 {
				sseRequest.Type = crypto.S3.String()
			}
			//TODO:add kms
		}
	}

	storageClass, err := getStorageClassFromHeader(headerfiedFormValues)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	result, err := api.ObjectAPI.PutObject(reqCtx, credential, -1, fileBody,
		metadata, acl, sseRequest, storageClass)
	if err != nil {
		logger.Error("Unable to create object", objectName, "error:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	isCallback, callbackMessage, err := GetCallbackFromForm(formValues)
	if err != nil {
		logger.Warn("Unable to get callback info with Complete multipart upload request:", reqCtx.ObjectName, "error:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	if isCallback {
		callbackMagicInfos := CallBackMagicInfos{
			BucketName: bucketName,
			FileName:   objectName,
			VersionId:  reqCtx.VersionId,
			Etag:       result.Md5,
			ObjectSize: result.ObjectSize,
			MimeType:   metadata["Content-Type"],
			CreateTime: uint64(result.LastModified.UnixNano()),
		}
		callbackMessage.Credential = credential
		resultCallback, err := api.CallbackProcess(callbackMagicInfos, callbackMessage, logger, credential)
		if err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
		w.Header().Add("x-uos-callback-result", resultCallback)
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
	for sc, v := range result.DeltaInfo {
		SetDeltaSize(w, sc, v)
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

func (api ObjectAPIHandlers) CallbackProcess(callbackMagicInfos CallBackMagicInfos, callbackMessage CallBackMessage, logger log.Logger, credential common.Credential) (result string, err error) {
	isNeedImageInfo, err := callbackMessage.IsCallbackImgNeedParse(callbackMagicInfos.MimeType, callbackMagicInfos.FileName)
	if err != nil {
		logger.Warn("Error callback image type with Complete multipart upload request:", callbackMagicInfos.FileName, "error:", err)
		return "", err
	}
	if isNeedImageInfo {
		objectInfo, err := api.ObjectAPI.GetObjectInfo(callbackMagicInfos.BucketName, callbackMagicInfos.FileName, callbackMagicInfos.VersionId, credential)
		if err != nil {
			logger.Warn("Complete multipart upload with callback failed get object info:", err)
			return "", ErrCallBackFailed
		}
		startOffset := int64(0)
		length := objectInfo.Size
		sse := SseRequest{
			Type: objectInfo.SseType,
		}
		pipeReader, pipeWriter := io.Pipe()
		go func() {
			err = api.ObjectAPI.GetObject(objectInfo, startOffset, length, pipeWriter, sse)
			if err != nil {
				logger.Warn("Complete multipart upload with callback failed get object info:", err)
				pipeWriter.CloseWithError(err)
				return
			}
			pipeWriter.Close()
		}()
		callbackMagicInfos.Height, callbackMagicInfos.Width, callbackMagicInfos.Format, err = GetImageInfoFromReader(pipeReader)
		if err != nil {
			return "", err
		}
	}
	callbackMessage, err = ParseCallbackInfos(callbackMagicInfos, callbackMessage)
	if err != nil {
		logger.Warn("PostObject with Callback err : Get callback info error")
		return "", err
	}
	result, err = PostCallbackMessage(callbackMessage)
	if err != nil {
		logger.Warn("PostObject with Callback err")
		return "", err
	}
	return
}
