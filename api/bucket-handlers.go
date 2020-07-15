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

package api

import (
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	meta "github.com/journeymidnight/yig/meta/types"

	. "github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	. "github.com/journeymidnight/yig/meta/common"
	"github.com/journeymidnight/yig/signature"
)

const (
	DeadLineForStandardIa = 720 * time.Hour  // 30 days
	DeadLineForGlacier    = 1440 * time.Hour // 60 days
)

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
func (api ObjectAPIHandlers) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpGetBucketLocation)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

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
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}

	if _, err = api.ObjectAPI.GetBucketInfo(reqCtx, credential); err != nil {
		logger.Error("Unable to fetch bucket info:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	// Generate response.
	encodedSuccessResponse := EncodeResponse(LocationResponse{
		Location: helper.CONFIG.Region,
	})

	WriteSuccessResponse(w, r, encodedSuccessResponse)
}

// ListMultipartUploadsHandler - GET Bucket (List Multipart uploads)
// -------------------------
// This operation lists in-progress multipart uploads. An in-progress
// multipart upload is a multipart upload that has been initiated,
// using the Initiate Multipart Upload request, but has not yet been
// completed or aborted. This operation returns at most 1,000 multipart
// uploads in the response.
//
func (api ObjectAPIHandlers) ListMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpListMultipartUploads)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

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

	request, err := parseListUploadsQuery(r.URL.Query())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	listMultipartsResponse, err := api.ObjectAPI.ListMultipartUploads(reqCtx, credential, request)
	if err != nil {
		logger.Error("Unable to list multipart uploads:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	encodedSuccessResponse := EncodeResponse(listMultipartsResponse)

	// write success response.
	WriteSuccessResponse(w, r, encodedSuccessResponse)
}

// ListObjectsHandler - GET Bucket (List Objects)
// -- -----------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
func (api ObjectAPIHandlers) ListObjectsHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpListObjects)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error

	if api.HandledByWebsite(w, r) {
		return
	}

	switch reqCtx.AuthType {
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

	request, err := parseListObjectsQuery(r.URL.Query())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	listObjectsInfo, err := api.ObjectAPI.ListObjects(reqCtx, credential, request)
	if err != nil {
		logger.Error("Unable to list objects:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	response := GenerateListObjectsResponse(reqCtx.BucketName, request, listObjectsInfo)
	encodedSuccessResponse := EncodeResponse(response)

	// Write success response.
	WriteSuccessResponse(w, r, encodedSuccessResponse)
	return
}

func (api ObjectAPIHandlers) ListVersionedObjectsHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpListVersionedObjects)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

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

	request, err := parseListObjectsQuery(r.URL.Query())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	request.Versioned = true

	listObjectsInfo, err := api.ObjectAPI.ListVersionedObjects(reqCtx, credential, request)
	if err != nil {
		logger.Error("Unable to list objects:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	response := GenerateVersionedListObjectResponse(reqCtx.BucketName, request, listObjectsInfo)
	encodedSuccessResponse := EncodeResponse(response)

	// Write success response.
	WriteSuccessResponse(w, r, encodedSuccessResponse)
	return
}

// ListBucketsHandler - GET Service
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api ObjectAPIHandlers) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpListBuckets)
	logger := ContextLogger(r)
	// List buckets does not support bucket policies.
	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	bucketsInfo, err := api.ObjectAPI.ListBuckets(credential)
	if err != nil {
		logger.Error("Unable to list buckets:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	// generate response
	response := GenerateListBucketsResponse(bucketsInfo, credential)
	encodedSuccessResponse := EncodeResponse(response)
	SetCredential(w, &credential)
	// write response
	WriteSuccessResponse(w, r, encodedSuccessResponse)
}

// DeleteMultipleObjectsHandler - deletes multiple objects.
func (api ObjectAPIHandlers) DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpDeleteMultipleObjects)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	switch reqCtx.AuthType {
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

	// Content-Length is required and should be non-zero
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	contentLength := r.ContentLength
	if contentLength <= 0 {
		WriteErrorResponse(w, r, ErrMissingContentLength)
		return
	}

	// Content-Md5 is required and should be set
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	contentMd5 := r.Header.Get("Content-Md5")
	if contentMd5 == "" {
		WriteErrorResponse(w, r, ErrMissingContentMD5)
		return
	}

	// Allocate incoming content length bytes.
	deleteXmlBytes := make([]byte, contentLength)

	// Read incoming body XML bytes.
	if n, err := io.ReadFull(r.Body, deleteXmlBytes); err != nil || int64(n) != contentLength {
		logger.Error("Unable to read HTTP body:", err)
		WriteErrorResponse(w, r, ErrIncompleteBody)
		return
	}

	// Unmarshal list of keys to be deleted.
	deleteObjects := &DeleteObjectsRequest{}
	if err := xml.Unmarshal(deleteXmlBytes, deleteObjects); err != nil {
		logger.Error("Unable to unmarshal delete objects request XML:", err)
		// FIXME? Amazon returns a 200 with error message XML
		WriteErrorResponse(w, r, ErrMalformedXML)
		return
	}

	var deleteErrors []DeleteError
	var deletedObjects []ObjectIdentifier

	var deltaResult = make([]int64, len(StorageClassIndexMap))

	var unexpiredInfo []UnexpiredTriple
	// Loop through all the objects and delete them sequentially.

	var wg = sync.WaitGroup{}
	deleteFunc := func(object ObjectIdentifier) {
		reqCtx.ObjectName = object.ObjectName
		reqCtx.VersionId = object.VersionId
		reqCtx.ObjectInfo, err = api.ObjectAPI.GetObjectInfo(reqCtx.BucketName, object.ObjectName, object.VersionId, credential)

		var result DeleteObjectResult
		if err == nil {
			result, err = api.ObjectAPI.DeleteObject(reqCtx, credential)
		}
		if err == nil {
			deletedObjects = append(deletedObjects, ObjectIdentifier{
				ObjectName:   object.ObjectName,
				VersionId:    object.VersionId,
				DeleteMarker: result.DeleteMarker,
				DeleteMarkerVersionId: helper.Ternary(result.DeleteMarker,
					result.VersionId, "").(string),
			})

			if ok, delta := isUnexpired(reqCtx.ObjectInfo); ok {
				unexpiredInfo = append(unexpiredInfo, UnexpiredTriple{
					StorageClass: reqCtx.ObjectInfo.StorageClass,
					Size:         CorrectDeltaSize(reqCtx.ObjectInfo.StorageClass, reqCtx.ObjectInfo.Size),
					SurvivalTime: delta,
				})
			}
			atomic.AddInt64(&deltaResult[result.DeltaSize.StorageClass], result.DeltaSize.Delta)
		} else if err != ErrNoSuchKey {
			logger.Error("Unable to delete object:", err)
			apiErrorCode, ok := err.(ApiErrorCode)
			if ok {
				deleteErrors = append(deleteErrors, DeleteError{
					Code:      ErrorCodeResponse[apiErrorCode].AwsErrorCode,
					Message:   ErrorCodeResponse[apiErrorCode].Description,
					Key:       object.ObjectName,
					VersionId: object.VersionId,
				})
			} else {
				deleteErrors = append(deleteErrors, DeleteError{
					Code:      "InternalError",
					Message:   "We encountered an internal error, please try again.",
					Key:       object.ObjectName,
					VersionId: object.VersionId,
				})
			}
		}
		wg.Done()
	}
	for _, object := range deleteObjects.Objects {
		wg.Add(1)
		go deleteFunc(object)
	}
	wg.Wait()
	for sc, v := range deltaResult {
		SetDeltaSize(w, StorageClass(sc), v)
	}
	SetUnexpiredInfo(w, unexpiredInfo)

	// Generate response
	response := GenerateMultiDeleteResponse(deleteObjects.Quiet, deletedObjects, deleteErrors)
	encodedSuccessResponse := EncodeResponse(response)

	// Write success response.
	WriteSuccessResponse(w, r, encodedSuccessResponse)
}

func isUnexpired(object *meta.Object) (bool, int64) {
	if object == nil {
		return false, 0
	}
	var expiredDeleteTime time.Time
	if object.StorageClass == ObjectStorageClassStandardIa {
		expiredDeleteTime = object.LastModifiedTime.Add(DeadLineForStandardIa)
	} else if object.StorageClass == ObjectStorageClassGlacier {
		expiredDeleteTime = object.LastModifiedTime.Add(DeadLineForGlacier)
	}
	// if delta < 0 ,the object should be record as unexpired
	delta := time.Now().UTC().Sub(expiredDeleteTime).Nanoseconds()
	// transfer to second
	delta /= 1e9
	if delta < 0 {
		return true, -delta
	}
	return false, 0
}

// PutBucketHandler - PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (api ObjectAPIHandlers) PutBucketHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpPutBucket)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	bucketName := reqCtx.BucketName
	if err := CheckValidBucketName(bucketName); err != nil {
		WriteErrorResponse(w, r, ErrInvalidBucketName)
		return
	}

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	if len(r.Header.Get("Content-Length")) == 0 {
		logger.Info("Content Length is null")
		WriteErrorResponse(w, r, ErrInvalidHeader)
		return
	}

	acl, err := getAclFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// TODO:the location value in the request body should match the Region in serverConfig.

	// Make bucket.
	err = api.ObjectAPI.MakeBucket(reqCtx, acl, credential)
	if err != nil {
		logger.Error("Unable to create bucket", bucketName, "error:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	// Make sure to add Location information here only for bucket
	w.Header().Set("Location", GetLocation(r))
	SetCredential(w, &credential)
	WriteSuccessResponse(w, r, nil)
}

func (api ObjectAPIHandlers) PutBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpPutBucketLogging)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	var bl BucketLoggingStatus
	blBuffer, err := ioutil.ReadAll(io.LimitReader(r.Body, 4096))
	if err != nil {
		logger.Error("Unable to read bucket logging body:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
	err = xml.Unmarshal(blBuffer, &bl)
	if err != nil {
		logger.Error("Unable to parse bucket logging XML body:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
	logger.Info("Setting bucket logging:", bl)

	if bl.LoggingEnabled.TargetBucket != "" {
		bucket, err := api.ObjectAPI.GetBucket(bl.LoggingEnabled.TargetBucket)
		if err != nil {
			if err == ErrNoSuchBucket {
				WriteErrorResponse(w, r, ErrInvalidTargetBucket)
				return
			} else {
				logger.Error("Unable to get bucket :", err)
				WriteErrorResponse(w, r, ErrInternalError)
				return
			}
			//TODO: Maybe support someone else's permissions
		} else if bucket.OwnerId != reqCtx.BucketInfo.OwnerId {
			WriteErrorResponse(w, r, ErrInvalidTargetBucket)
			return
		}

		if reqCtx.BucketInfo.BucketLogging.LoggingEnabled.TargetBucket == "" || reqCtx.BucketInfo.BucketLogging.SetLog == false { // set bucket log first time
			bl.SetTime = time.Now().Format(timeLayoutStr)
			bl.SetLog = true
		} else {
			bl.SetTime = reqCtx.BucketInfo.BucketLogging.SetTime
			bl.SetLog = true
		}
	} else {
		bl.LoggingEnabled = reqCtx.BucketInfo.BucketLogging.LoggingEnabled
		bl.DeleteTime = time.Now().Format(timeLayoutStr)
		bl.SetLog = false
	}

	err = api.ObjectAPI.SetBucketLogging(reqCtx, bl, credential)
	if err != nil {
		logger.Error(err, "Unable to set bucket logging for bucket:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	WriteSuccessResponse(w, r, nil)
}

func (api ObjectAPIHandlers) GetBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpGetBucketLogging)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	bucketName := reqCtx.BucketName

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

	if reqCtx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return
	}

	if credential.UserId != reqCtx.BucketInfo.OwnerId {
		WriteErrorResponse(w, r, ErrBucketAccessForbidden)
		return
	}

	bl, err := api.ObjectAPI.GetBucketLogging(reqCtx, credential)
	if err != nil {
		logger.Error("Failed to get bucket ACL policy for bucket", bucketName,
			"error:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	if bl.SetLog == false {
		bl.LoggingEnabled.TargetBucket = ""
		bl.LoggingEnabled.TargetPrefix = ""
	}
	blBuffer, err := xmlFormat(bl)
	if err != nil {
		logger.Error("Failed to marshal bucket logging XML for bucket", bucketName,
			"error:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w)

	WriteSuccessResponse(w, r, blBuffer)

}

func (api ObjectAPIHandlers) PutBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpPutBucketAcl)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	var acl Acl
	var policy AccessControlPolicy
	if _, ok := r.Header["X-Amz-Acl"]; ok {
		acl, err = getAclFromHeader(r.Header)
		if err != nil {
			logger.Error("Unable to read canned ACLs:", err)
			WriteErrorResponse(w, r, ErrInvalidAcl)
			return
		}
	} else {
		aclBuffer, err := ioutil.ReadAll(io.LimitReader(r.Body, 1024))
		if err != nil {
			logger.Error("Unable to read ACL body:", err)
			WriteErrorResponse(w, r, ErrInvalidAcl)
			return
		}
		err = xml.Unmarshal(aclBuffer, &policy)
		if err != nil {
			logger.Error("Unable to parse ACLs XML body:", err)
			WriteErrorResponse(w, r, ErrInternalError)
			return
		}
	}

	err = api.ObjectAPI.SetBucketAcl(reqCtx, policy, acl, credential)
	if err != nil {
		logger.Error("Unable to set ACL for bucket:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	WriteSuccessResponse(w, r, nil)
}

func (api ObjectAPIHandlers) GetBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpGetBucketAcl)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	bucketName := reqCtx.BucketName

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

	policy, err := api.ObjectAPI.GetBucketAcl(reqCtx, credential)
	if err != nil {
		logger.Error("Failed to get ACL policy for bucket", bucketName,
			"error:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	aclBuffer, err := xmlFormat(policy)
	if err != nil {
		logger.Error("Failed to marshal ACL XML for bucket", bucketName,
			"error:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w)

	WriteSuccessResponse(w, r, aclBuffer)
}

func (api ObjectAPIHandlers) PutBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpPutBucketCors)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// If Content-Length is unknown or zero, deny the request.
	if !contains(r.TransferEncoding, "chunked") {
		if r.ContentLength == -1 || r.ContentLength == 0 {
			WriteErrorResponse(w, r, ErrMissingContentLength)
			return
		}
		// If Content-Length is greater than maximum allowed CORS size.
		if r.ContentLength > MAX_CORS_SIZE {
			WriteErrorResponse(w, r, ErrEntityTooLarge)
			return
		}
	}

	corsBuffer, err := ioutil.ReadAll(io.LimitReader(r.Body, MAX_CORS_SIZE))
	if err != nil {
		logger.Error("Unable to read CORS body:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	cors, err := CorsFromXml(corsBuffer)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	err = api.ObjectAPI.SetBucketCors(reqCtx, cors, credential)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	WriteSuccessResponse(w, r, nil)
}

func (api ObjectAPIHandlers) DeleteBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpDeleteBucketCors)
	reqCtx := GetRequestContext(r)

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	err = api.ObjectAPI.DeleteBucketCors(reqCtx, credential)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	WriteSuccessNoContent(w)
}

func (api ObjectAPIHandlers) GetBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpGetBucketCors)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	bucketName := reqCtx.BucketName

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	cors, err := api.ObjectAPI.GetBucketCors(reqCtx, credential)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	corsBuffer, err := xmlFormat(cors)
	if err != nil {
		logger.Error("Failed to marshal CORS XML for bucket", bucketName,
			"error:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w)

	WriteSuccessResponse(w, r, corsBuffer)
}

func (api ObjectAPIHandlers) GetBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpGetBucketVersioning)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	bucketName := reqCtx.BucketName

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	versioning, err := api.ObjectAPI.GetBucketVersioning(reqCtx, credential)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	versioningBuffer, err := xmlFormat(versioning)
	if err != nil {
		logger.Error(err, "Failed to marshal versioning XML for bucket", bucketName,
			"error:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w)

	WriteSuccessResponse(w, r, versioningBuffer)
}

func (api ObjectAPIHandlers) PutBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpPutBucketVersioning)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// If Content-Length is unknown or zero, deny the request.
	if !contains(r.TransferEncoding, "chunked") {
		if r.ContentLength == -1 || r.ContentLength == 0 {
			WriteErrorResponse(w, r, ErrMissingContentLength)
			return
		}
		// If Content-Length is greater than 1024
		// Since the versioning XML is usually small, 1024 is a reasonable limit
		if r.ContentLength > 1024 {
			WriteErrorResponse(w, r, ErrEntityTooLarge)
			return
		}
	}

	versioningBuffer, err := ioutil.ReadAll(io.LimitReader(r.Body, 1024))
	if err != nil {
		logger.Error("Unable to read versioning body:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	versioning, err := VersioningFromXml(versioningBuffer)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	err = api.ObjectAPI.SetBucketVersioning(reqCtx, versioning, credential)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	WriteSuccessResponse(w, r, nil)
}

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (api ObjectAPIHandlers) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpHeadBucket)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

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

	if _, err = api.ObjectAPI.GetBucketInfo(reqCtx, credential); err != nil {
		logger.Error("Unable to fetch bucket info:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	WriteSuccessResponse(w, r, nil)
}

// DeleteBucketHandler - Delete bucket
func (api ObjectAPIHandlers) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpDeleteBucket)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	if err = api.ObjectAPI.DeleteBucket(reqCtx, credential); err != nil {
		logger.Error("Unable to delete a bucket:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	// Write success response.
	WriteSuccessNoContent(w)
}
