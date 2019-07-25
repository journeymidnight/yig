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
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	. "github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/signature"
)

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
func (api ObjectAPIHandlers) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

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

	if _, err = api.ObjectAPI.GetBucketInfo(bucketName, credential, r.Context()); err != nil {
		helper.ErrorIf(err, "Unable to fetch bucket info.")
		WriteErrorResponse(w, r, err)
		return
	}

	// Generate response.
	encodedSuccessResponse := EncodeResponse(LocationResponse{
		Location: helper.CONFIG.Region,
	})
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
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

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

	listMultipartsResponse, err := api.ObjectAPI.ListMultipartUploads(credential, bucketName, request, r.Context())
	if err != nil {
		helper.ErrorIf(err, "Unable to list multipart uploads.")
		WriteErrorResponse(w, r, err)
		return
	}
	encodedSuccessResponse := EncodeResponse(listMultipartsResponse)
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
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

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

	request, err := parseListObjectsQuery(r.URL.Query(), r.Context())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	listObjectsInfo, err := api.ObjectAPI.ListObjects(credential, bucketName, request, r.Context())
	if err != nil {
		helper.ErrorIf(err, "Unable to list objects.")
		WriteErrorResponse(w, r, err)
		return
	}

	response := GenerateListObjectsResponse(bucketName, request, listObjectsInfo)
	encodedSuccessResponse := EncodeResponse(response)

	// Write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
	return
}

func (api ObjectAPIHandlers) ListVersionedObjectsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

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

	request, err := parseListObjectsQuery(r.URL.Query(), r.Context())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	request.Versioned = true

	listObjectsInfo, err := api.ObjectAPI.ListVersionedObjects(credential, bucketName, request, r.Context())
	if err != nil {
		helper.ErrorIf(err, "Unable to list objects.")
		WriteErrorResponse(w, r, err)
		return
	}

	response := GenerateVersionedListObjectResponse(bucketName, request, listObjectsInfo)
	encodedSuccessResponse := EncodeResponse(response)

	// Write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
	return
}

// ListBucketsHandler - GET Service
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api ObjectAPIHandlers) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	// List buckets does not support bucket policies.
	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	bucketsInfo, err := api.ObjectAPI.ListBuckets(credential, r.Context())
	if err == nil {
		// generate response
		response := GenerateListBucketsResponse(bucketsInfo, credential)
		encodedSuccessResponse := EncodeResponse(response)
		// write response
		WriteSuccessResponse(w, encodedSuccessResponse)
		return
	}
	helper.ErrorIf(err, "Unable to list buckets.")
	WriteErrorResponse(w, r, err)
}

// DeleteMultipleObjectsHandler - deletes multiple objects.
func (api ObjectAPIHandlers) DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

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
		helper.ErrorIf(err, "Unable to read HTTP body.")
		WriteErrorResponse(w, r, ErrIncompleteBody)
		return
	}

	// Unmarshal list of keys to be deleted.
	deleteObjects := &DeleteObjectsRequest{}
	if err := xml.Unmarshal(deleteXmlBytes, deleteObjects); err != nil {
		helper.ErrorIf(err, "Unable to unmarshal delete objects request XML.")
		// FIXME? Amazon returns a 200 with error message XML
		WriteErrorResponse(w, r, ErrMalformedXML)
		return
	}

	var deleteErrors []DeleteError
	var deletedObjects []ObjectIdentifier
	// Loop through all the objects and delete them sequentially.
	for _, object := range deleteObjects.Objects {
		result, err := api.ObjectAPI.DeleteObject(bucket, object.ObjectName,
			object.VersionId, credential, r.Context())
		if err == nil {
			deletedObjects = append(deletedObjects, ObjectIdentifier{
				ObjectName:   object.ObjectName,
				VersionId:    object.VersionId,
				DeleteMarker: result.DeleteMarker,
				DeleteMarkerVersionId: helper.Ternary(result.DeleteMarker,
					result.VersionId, "").(string),
			})
		} else {
			helper.ErrorIf(err, "Unable to delete object.")
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
	// Write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
}

// PutBucketHandler - PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (api ObjectAPIHandlers) PutBucketHandler(w http.ResponseWriter, r *http.Request) {
	helper.Debugln("[", RequestIdFromContext(r.Context()), "]", "PutBucketHandler", "enter")
	vars := mux.Vars(r)
	bucketName := strings.ToLower(vars["bucket"])
	if !isValidBucketName(bucketName) {
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
		helper.Debugln("[", RequestIdFromContext(r.Context()), "]", "Content Length is null!")
		WriteErrorResponse(w, r, ErrInvalidHeader)
		return
	}

	acl, err := getAclFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// TODO:the location value in the request body should match the Region in serverConfig.
	// other values of location are not accepted.
	// make bucket fails in such cases.

	//	err = isValidLocationConstraint(r.Body)
	//	if err != nil {
	//		WriteErrorResponse(w, r, err)
	//		return
	//	}

	// Make bucket.
	err = api.ObjectAPI.MakeBucket(bucketName, acl, credential, r.Context())
	if err != nil {
		helper.ErrorIf(err, "Unable to create bucket "+bucketName)
		WriteErrorResponse(w, r, err)
		return
	}
	// Make sure to add Location information here only for bucket
	w.Header().Set("Location", GetLocation(r))
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) PutBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	helper.Logger.Println(10, "[", RequestIdFromContext(r.Context()), "]", "enter PutBucketLCHandler")
	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	var lc Lc
	lcBuffer, err := ioutil.ReadAll(io.LimitReader(r.Body, 4096))
	if err != nil {
		helper.ErrorIf(err, "Unable to read lifecycle body")
		WriteErrorResponse(w, r, ErrInvalidLc)
		return
	}
	err = xml.Unmarshal(lcBuffer, &lc)
	if err != nil {
		helper.ErrorIf(err, "Unable to parse lifecycle xml body")
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	helper.Debugln("[", RequestIdFromContext(r.Context()), "]", "Set LC:", lc)
	err = api.ObjectAPI.SetBucketLc(bucket, lc, credential, r.Context())
	if err != nil {
		helper.ErrorIf(err, "Unable to set LC for bucket.")
		WriteErrorResponse(w, r, err)
		return
	}
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) GetBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

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

	lc, err := api.ObjectAPI.GetBucketLc(bucketName, credential, r.Context())
	if err != nil {
		helper.ErrorIf(err, "Failed to get bucket acl policy for bucket", bucketName)
		WriteErrorResponse(w, r, err)
		return
	}

	lcBuffer, err := xmlFormat(lc)
	if err != nil {
		helper.ErrorIf(err, "Failed to marshal lc XML for bucket", bucketName)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w, lcBuffer)
	WriteSuccessResponse(w, lcBuffer)

}

func (api ObjectAPIHandlers) DelBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	err = api.ObjectAPI.DelBucketLc(bucketName, credential, r.Context())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	WriteSuccessNoContent(w)

}

func (api ObjectAPIHandlers) PutBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

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
			helper.ErrorIf(err, "Unable to read canned acls")
			WriteErrorResponse(w, r, ErrInvalidAcl)
			return
		}
	} else {
		aclBuffer, err := ioutil.ReadAll(io.LimitReader(r.Body, 1024))
		if err != nil {
			helper.ErrorIf(err, "Unable to read acls body")
			WriteErrorResponse(w, r, ErrInvalidAcl)
			return
		}
		err = xml.Unmarshal(aclBuffer, &policy)
		if err != nil {
			helper.ErrorIf(err, "Unable to parse acls xml body")
			WriteErrorResponse(w, r, ErrInternalError)
			return
		}
	}

	err = api.ObjectAPI.SetBucketAcl(bucket, policy, acl, credential, r.Context())
	if err != nil {
		helper.ErrorIf(err, "Unable to set ACL for bucket.")
		WriteErrorResponse(w, r, err)
		return
	}
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) GetBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

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

	policy, err := api.ObjectAPI.GetBucketAcl(bucketName, credential, r.Context())
	if err != nil {
		helper.ErrorIf(err, "Failed to get bucket acl policy for bucket", bucketName)
		WriteErrorResponse(w, r, err)
		return
	}

	aclBuffer, err := xmlFormat(policy)
	if err != nil {
		helper.ErrorIf(err, "Failed to marshal acl XML for bucket", bucketName)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w, aclBuffer)
	WriteSuccessResponse(w, aclBuffer)
}

func (api ObjectAPIHandlers) PutBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

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
		helper.ErrorIf(err, "Unable to read CORS body")
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	cors, err := CorsFromXml(corsBuffer, r.Context())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	err = api.ObjectAPI.SetBucketCors(bucketName, cors, credential, r.Context())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) DeleteBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	err = api.ObjectAPI.DeleteBucketCors(bucketName, credential, r.Context())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	WriteSuccessNoContent(w)
}

func (api ObjectAPIHandlers) GetBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	cors, err := api.ObjectAPI.GetBucketCors(bucketName, credential, r.Context())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	corsBuffer, err := xmlFormat(cors)
	if err != nil {
		helper.ErrorIf(err, "Failed to marshal CORS XML for bucket", bucketName)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w, corsBuffer)
	WriteSuccessResponse(w, corsBuffer)
}

func (api ObjectAPIHandlers) GetBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	versioning, err := api.ObjectAPI.GetBucketVersioning(bucketName, credential, r.Context())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	versioningBuffer, err := xmlFormat(versioning)
	if err != nil {
		helper.ErrorIf(err, "Failed to marshal versioning XML for bucket", bucketName)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w, versioningBuffer)
	WriteSuccessResponse(w, versioningBuffer)
}

func (api ObjectAPIHandlers) PutBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

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
		helper.ErrorIf(err, "Unable to read versioning body")
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	versioning, err := VersioningFromXml(versioningBuffer)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	err = api.ObjectAPI.SetBucketVersioning(bucketName, versioning, credential, r.Context())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	WriteSuccessResponse(w, nil)
}

func extractHTTPFormValues(reader *multipart.Reader) (filePartReader io.Reader,
	formValues map[string]string, err error) {

	formValues = make(map[string]string)
	for {
		var part *multipart.Part
		part, err = reader.NextPart()
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return nil, nil, err
		}

		if part.FormName() != "file" {
			var buffer []byte
			buffer, err = ioutil.ReadAll(part)
			if err != nil {
				return nil, nil, err
			}
			formValues[http.CanonicalHeaderKey(part.FormName())] = string(buffer)
		} else {
			// "All variables within the form are expanded prior to validating
			// the POST policy"
			fileName := part.FileName()
			objectKey, ok := formValues["Key"]
			if !ok {
				return nil, nil, ErrMissingFields
			}
			if strings.Contains(objectKey, "${filename}") {
				formValues["Key"] = strings.Replace(objectKey, "${filename}", fileName, -1)
			}

			filePartReader = part
			// "The file or content must be the last field in the form.
			// Any fields below it are ignored."
			break
		}
	}

	if filePartReader == nil {
		err = ErrEmptyEntity
	}
	return
}

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (api ObjectAPIHandlers) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

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

	if _, err = api.ObjectAPI.GetBucketInfo(bucket, credential, r.Context()); err != nil {
		helper.ErrorIf(err, "Unable to fetch bucket info.")
		WriteErrorResponse(w, r, err)
		return
	}
	WriteSuccessResponse(w, nil)
}

// DeleteBucketHandler - Delete bucket
func (api ObjectAPIHandlers) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	if err = api.ObjectAPI.DeleteBucket(bucket, credential, r.Context()); err != nil {
		helper.ErrorIf(err, "Unable to delete a bucket.")
		WriteErrorResponse(w, r, err)
		return
	}

	// Write success response.
	WriteSuccessNoContent(w)
}
