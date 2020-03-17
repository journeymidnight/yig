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

	. "github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/signature"
)

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
func (api ObjectAPIHandlers) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
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
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "GetBucketLocation"
	WriteSuccessResponse(w, encodedSuccessResponse)
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
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "ListMultipartUploads"
	// write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
}

// ListObjectsHandler - GET Bucket (List Objects)
// -- -----------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
func (api ObjectAPIHandlers) ListObjectsHandler(w http.ResponseWriter, r *http.Request) {
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

	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "ListObjects"
	// Write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
	return
}

func (api ObjectAPIHandlers) ListVersionedObjectsHandler(w http.ResponseWriter, r *http.Request) {
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

	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "ListVersionedObjects"
	// Write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
	return
}

// ListBucketsHandler - GET Service
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api ObjectAPIHandlers) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
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
	// write response
	WriteSuccessResponse(w, encodedSuccessResponse)
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "ListBuckets"
	return
}

// DeleteMultipleObjectsHandler - deletes multiple objects.
func (api ObjectAPIHandlers) DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {
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
	// Loop through all the objects and delete them sequentially.
	for _, object := range deleteObjects.Objects {
		result, err := api.ObjectAPI.DeleteObject(reqCtx, credential)
		if err == nil {
			deletedObjects = append(deletedObjects, ObjectIdentifier{
				ObjectName:   object.ObjectName,
				VersionId:    object.VersionId,
				DeleteMarker: result.DeleteMarker,
				DeleteMarkerVersionId: helper.Ternary(result.DeleteMarker,
					result.VersionId, "").(string),
			})
		} else {
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
	}
	// Generate response
	response := GenerateMultiDeleteResponse(deleteObjects.Quiet, deletedObjects, deleteErrors)
	encodedSuccessResponse := EncodeResponse(response)
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "DeleteMultipleObjects"
	// Write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
}

// PutBucketHandler - PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (api ObjectAPIHandlers) PutBucketHandler(w http.ResponseWriter, r *http.Request) {
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
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucket"
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) PutBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
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
		WriteErrorResponse(w, r, ErrInvalidBucketLogging)
		return
	}
	err = xml.Unmarshal(blBuffer, &bl)
	if err != nil {
		logger.Error("Unable to parse bucket logging XML body:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
	logger.Info("Setting bucket logging:", bl)
	err = api.ObjectAPI.SetBucketLogging(reqCtx, bl, credential)
	if err != nil {
		logger.Error(err, "Unable to set bucket logging for bucket:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucketLogging"
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) GetBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
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

	blBuffer, err := xmlFormat(bl)
	if err != nil {
		logger.Error("Failed to marshal bucket logging XML for bucket", bucketName,
			"error:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w)
	//ResponseRecorder
	w.(*ResponseRecorder).operationName = "GetBucketLogging"
	WriteSuccessResponse(w, blBuffer)

}

func (api ObjectAPIHandlers) PutBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	var lc Lifecycle
	lcBuffer, err := ioutil.ReadAll(io.LimitReader(r.Body, 4096))
	if err != nil {
		logger.Error("Unable to read lifecycle body:", err)
		WriteErrorResponse(w, r, ErrInvalidLc)
		return
	}
	err = xml.Unmarshal(lcBuffer, &lc)
	if err != nil {
		logger.Error("Unable to parse lifecycle XML body:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	logger.Info("Setting lifecycle:", lc)
	err = api.ObjectAPI.SetBucketLifecycle(reqCtx, lc, credential)
	if err != nil {
		logger.Error(err, "Unable to set lifecycle for bucket:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucketLifeCycle"
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) GetBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
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

	lc, err := api.ObjectAPI.GetBucketLifecycle(reqCtx, credential)
	if err != nil {
		logger.Error("Failed to get bucket ACL policy for bucket", bucketName,
			"error:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	lcBuffer, err := xmlFormat(lc)
	if err != nil {
		logger.Error("Failed to marshal lifecycle XML for bucket", bucketName,
			"error:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w)
	//ResponseRecorder
	w.(*ResponseRecorder).operationName = "GetBucketLifeCycle"
	WriteSuccessResponse(w, lcBuffer)

}

func (api ObjectAPIHandlers) DelBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	err = api.ObjectAPI.DelBucketLifecycle(reqCtx, credential)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "DelBucketLifeCycle"
	WriteSuccessNoContent(w)

}

func (api ObjectAPIHandlers) PutBucketAclHandler(w http.ResponseWriter, r *http.Request) {
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
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucketAcl"
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) GetBucketAclHandler(w http.ResponseWriter, r *http.Request) {
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
	//ResponseRecorder
	w.(*ResponseRecorder).operationName = "GetBucketAcl"
	WriteSuccessResponse(w, aclBuffer)
}

func (api ObjectAPIHandlers) PutBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
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
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucketCors"
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) DeleteBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
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
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "DeleteBucketCors"
	WriteSuccessNoContent(w)
}

func (api ObjectAPIHandlers) GetBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
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
	//ResponseRecorder
	w.(*ResponseRecorder).operationName = "GetBucketCors"
	WriteSuccessResponse(w, corsBuffer)
}

func (api ObjectAPIHandlers) GetBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
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
	//ResponseRecorder
	w.(*ResponseRecorder).operationName = "GetBucketVersioning"
	WriteSuccessResponse(w, versioningBuffer)
}

func (api ObjectAPIHandlers) PutBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
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
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucketVersioning"
	WriteSuccessResponse(w, nil)
}

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (api ObjectAPIHandlers) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
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
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "HeadBucket"
	WriteSuccessResponse(w, nil)
}

// DeleteBucketHandler - Delete bucket
func (api ObjectAPIHandlers) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
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

	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "DeleteBucket"
	// Write success response.
	WriteSuccessNoContent(w)
}
