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
	"time"

	. "github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/brand"
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
	switch signature.GetRequestAuthType(r, reqCtx.Brand) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypeSignedV4, signature.AuthTypePresignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			e, logLevel := ParseError(err)
			logger.Log(logLevel, "GetBucketLocationHandler signature authenticate err:", err)
			WriteErrorResponse(w, r, e)
			return
		}
	}

	if _, err = api.ObjectAPI.GetBucketInfo(reqCtx, credential); err != nil {
		logger.Warn("Unable to fetch bucket info:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	// Generate response.
	encodedSuccessResponse := EncodeResponse(LocationResponse{
		Location: helper.CONFIG.Region,
	})
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "GetBucketLocation"
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
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	switch signature.GetRequestAuthType(r, reqCtx.Brand) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			e, logLevel := ParseError(err)
			logger.Log(logLevel, "ListMultipartUploadsHandler signature authenticate err:", err)
			WriteErrorResponse(w, r, e)
			return
		}
	}

	request, err := parseListUploadsQuery(r.URL.Query())
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "ListMultipartUploadsHandler parseListUploadsQuery err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	listMultipartsResponse, err := api.ObjectAPI.ListMultipartUploads(reqCtx, credential, request)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Unable to list multipart uploads:", err)
		WriteErrorResponse(w, r, e)
		return
	}
	encodedSuccessResponse := EncodeResponse(listMultipartsResponse)
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "ListMultipartUploads"
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
			e, logLevel := ParseError(err)
			logger.Log(logLevel, "ListObjectsHandler signature authenticate err:", err)
			WriteErrorResponse(w, r, e)
			return
		}
	}

	request, err := parseListObjectsQuery(r.URL.Query())
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "ListObjectsHandler parseListObjectsQuery err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	listObjectsInfo, err := api.ObjectAPI.ListObjects(reqCtx, credential, request)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Unable to list objects:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	response := GenerateListObjectsResponse(reqCtx.BucketName, request, listObjectsInfo)
	encodedSuccessResponse := EncodeResponse(response)

	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "ListObjects"
	// Write success response.
	WriteSuccessResponse(w, r, encodedSuccessResponse)
	return
}

func (api ObjectAPIHandlers) ListVersionedObjectsHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	switch signature.GetRequestAuthType(r, reqCtx.Brand) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypeSignedV4, signature.AuthTypePresignedV4,
		signature.AuthTypeSignedV2, signature.AuthTypePresignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			e, logLevel := ParseError(err)
			logger.Log(logLevel, "ListVersionedObjectsHandler signature authenticate err:", err)
			WriteErrorResponse(w, r, e)
			return
		}
	}

	request, err := parseListObjectsQuery(r.URL.Query())
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "ListVersionedObjectsHandler parseListObjectsQuery err:", err)
		WriteErrorResponse(w, r, e)
		return
	}
	request.Versioned = true

	listObjectsInfo, err := api.ObjectAPI.ListVersionedObjects(reqCtx, credential, request)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Unable to list objects:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	response := GenerateVersionedListObjectResponse(reqCtx.BucketName, request, listObjectsInfo)
	encodedSuccessResponse := EncodeResponse(response)

	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "ListVersionedObjects"
	// Write success response.
	WriteSuccessResponse(w, r, encodedSuccessResponse)
	return
}

// ListBucketsHandler - GET Service
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api ObjectAPIHandlers) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	logger := GetContextLogger(r)
	// List buckets does not support bucket policies.
	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "ListBucketsHandler signature authenticate err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	bucketsInfo, err := api.ObjectAPI.ListBuckets(credential)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Unable to list buckets:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	// generate response
	response := GenerateListBucketsResponse(bucketsInfo, credential)
	encodedSuccessResponse := EncodeResponse(response)
	// write response
	WriteSuccessResponse(w, r, encodedSuccessResponse)
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
			e, logLevel := ParseError(err)
			logger.Log(logLevel, "DeleteMultipleObjectsHandler signature authenticate err:", err)
			WriteErrorResponse(w, r, e)
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

	result, err := api.ObjectAPI.DeleteObjects(reqCtx, credential, deleteObjects.Objects)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Unable to delete objects:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	// Generate response
	response := GenerateMultiDeleteResponse(deleteObjects.Quiet, result.DeletedObjects, result.DeleteErrors)
	encodedSuccessResponse := EncodeResponse(response)
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "DeleteMultipleObjects"
	// Write success response.
	WriteSuccessResponse(w, r, encodedSuccessResponse)
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
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "PutBucketHandler signature authenticate err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	if len(r.Header.Get("Content-Length")) == 0 {
		logger.Info("Content Length is null")
		WriteErrorResponse(w, r, ErrInvalidHeader)
		return
	}

	acl, err := getAclFromHeader(r.Header, reqCtx.Brand)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// TODO:the location value in the request body should match the Region in serverConfig.

	// Make bucket.
	err = api.ObjectAPI.MakeBucket(reqCtx, acl, credential)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Unable to create bucket", bucketName, "error:", err)
		WriteErrorResponse(w, r, e)
		return
	}
	// Make sure to add Location information here only for bucket
	w.Header().Set("Location", GetLocation(r))
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucket"
	WriteSuccessResponse(w, r, nil)
}

func (api ObjectAPIHandlers) PutBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "PutBucketLoggingHandler signature authenticate err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	var bl BucketLoggingStatus
	blBuffer, err := ioutil.ReadAll(io.LimitReader(r.Body, 4096))
	if err != nil {
		logger.Fatal("Unable to read bucket logging body:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
	err = xml.Unmarshal(blBuffer, &bl)
	if err != nil {
		logger.Error("Unable to parse bucket logging XML body:", err)
		WriteErrorResponse(w, r, ErrMalformedXML)
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
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Unable to set bucket logging for bucket:", err)
		WriteErrorResponse(w, r, e)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucketLogging"
	WriteSuccessResponse(w, r, nil)
}

func (api ObjectAPIHandlers) GetBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	bucketName := reqCtx.BucketName

	var credential common.Credential
	var err error
	switch signature.GetRequestAuthType(r, reqCtx.Brand) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			e, logLevel := ParseError(err)
			logger.Log(logLevel, "GetBucketLoggingHandler signature authenticate err:", err)
			WriteErrorResponse(w, r, e)
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
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Failed to get bucket ACL policy for bucket", bucketName, " error:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	if bl.SetLog == false {
		bl.LoggingEnabled.TargetBucket = ""
		bl.LoggingEnabled.TargetPrefix = ""
	}
	blBuffer, err := xmlFormat(bl)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Failed to marshal bucket logging XML for bucket:", bucketName, "error:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	setXmlHeader(w)
	//ResponseRecorder
	w.(*ResponseRecorder).operationName = "GetBucketLogging"
	WriteSuccessResponse(w, r, blBuffer)

}

func (api ObjectAPIHandlers) PutBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "PutBucketAclHandler signature authenticate err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	var acl Acl
	var policy AccessControlPolicy
	if _, ok := r.Header[reqCtx.Brand.GetHeaderFieldKey(brand.XACL)]; ok {
		acl, err = getAclFromHeader(r.Header, reqCtx.Brand)
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
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Unable to set ACL for bucket:", err)
		WriteErrorResponse(w, r, e)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucketAcl"
	WriteSuccessResponse(w, r, nil)
}

func (api ObjectAPIHandlers) GetBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	bucketName := reqCtx.BucketName

	var credential common.Credential
	var err error
	switch signature.GetRequestAuthType(r, reqCtx.Brand) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			e, logLevel := ParseError(err)
			logger.Log(logLevel, "GetBucketAclHandler signature authenticate err:", err)
			WriteErrorResponse(w, r, e)
			return
		}
	}

	policy, err := api.ObjectAPI.GetBucketAcl(reqCtx, credential)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Failed to get ACL policy for bucket", bucketName, "error:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	aclBuffer, err := xmlFormat(policy)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Failed to marshal ACL XML for bucket", bucketName, "error:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	setXmlHeader(w)
	//ResponseRecorder
	w.(*ResponseRecorder).operationName = "GetBucketAcl"
	WriteSuccessResponse(w, r, aclBuffer)
}

func (api ObjectAPIHandlers) PutBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "PutBucketCorsHandler signature authenticate err:", err)
		WriteErrorResponse(w, r, e)
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
		logger.Fatal("Unable to read CORS body:", err)
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
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "PutBucketCorsHandler set cors err:", err)
		WriteErrorResponse(w, r, e)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucketCors"
	WriteSuccessResponse(w, r, nil)
}

func (api ObjectAPIHandlers) DeleteBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "DeleteBucketCorsHandler signature authenticate err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	err = api.ObjectAPI.DeleteBucketCors(reqCtx, credential)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "DeleteBucketCorsHandler delete cors err:", err)
		WriteErrorResponse(w, r, e)
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
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "GetBucketCorsHandler signature authenticate err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	cors, err := api.ObjectAPI.GetBucketCors(reqCtx, credential)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "GetBucketCorsHandler get cors err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	corsBuffer, err := xmlFormat(cors)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Failed to marshal CORS XML for bucket", bucketName, "error:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	setXmlHeader(w)
	//ResponseRecorder
	w.(*ResponseRecorder).operationName = "GetBucketCors"
	WriteSuccessResponse(w, r, corsBuffer)
}

func (api ObjectAPIHandlers) GetBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	bucketName := reqCtx.BucketName

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "GetBucketVersioningHandler signature authenticate err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	versioning, err := api.ObjectAPI.GetBucketVersioning(reqCtx, credential)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "GetBucketVersioningHandler get versioning err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	versioningBuffer, err := xmlFormat(versioning)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Failed to marshal versioning XML for bucket:", bucketName, "error:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	setXmlHeader(w)
	//ResponseRecorder
	w.(*ResponseRecorder).operationName = "GetBucketVersioning"
	WriteSuccessResponse(w, r, versioningBuffer)
}

func (api ObjectAPIHandlers) PutBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "PutBucketVersioningHandler signature authenticate err:", err)
		WriteErrorResponse(w, r, e)
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
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "PutBucketVersioningHandler set versioning err:", err)
		WriteErrorResponse(w, r, e)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucketVersioning"
	WriteSuccessResponse(w, r, nil)
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
	switch signature.GetRequestAuthType(r, reqCtx.Brand) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			e, logLevel := ParseError(err)
			logger.Log(logLevel, "HeadBucketHandler signature authenticate err:", err)
			WriteErrorResponse(w, r, e)
			return
		}
	}

	if _, err = api.ObjectAPI.GetBucketInfo(reqCtx, credential); err != nil {
		logger.Warn("Unable to fetch bucket info:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "HeadBucket"
	WriteSuccessResponse(w, r, nil)
}

// DeleteBucketHandler - Delete bucket
func (api ObjectAPIHandlers) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "DeleteBucketHandler signature authenticate err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	if err = api.ObjectAPI.DeleteBucket(reqCtx, credential); err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Unable to delete a bucket:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "DeleteBucket"
	// Write success response.
	WriteSuccessNoContent(w)
}
