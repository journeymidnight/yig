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
	"net/url"
	"strings"

	. "git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/iam"
	"git.letv.cn/yig/yig/signature"
	mux "github.com/gorilla/mux"
	"strconv"
)

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
func (api ObjectAPIHandlers) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

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
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}

	if _, err = api.ObjectAPI.GetBucketInfo(bucketName, credential); err != nil {
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

	request, err := parseListUploadsQuery(r.URL.Query())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	listMultipartsResponse, err := api.ObjectAPI.ListMultipartUploads(credential, bucketName, request)
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

	request, err := parseListObjectsQuery(r.URL.Query())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	listObjectsInfo, err := api.ObjectAPI.ListObjects(credential, bucketName, request)
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

	request, err := parseListObjectsQuery(r.URL.Query())
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	request.Versioned = true

	listObjectsInfo, err := api.ObjectAPI.ListVersionedObjects(credential, bucketName, request)
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
	var credential iam.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	bucketsInfo, err := api.ObjectAPI.ListBuckets(credential)
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
			object.VersionId, credential)
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
	helper.Debugln("PutBucketHandler", "enter")
	vars := mux.Vars(r)
	bucketName := strings.ToLower(vars["bucket"])
	if !isValidBucketName(bucketName) {
		WriteErrorResponse(w, r, ErrInvalidBucketName)
		return
	}
	var credential iam.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	acl, err := getAclFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// the location value in the request body should match the Region in serverConfig.
	// other values of location are not accepted.
	// make bucket fails in such cases.
	err = isValidLocationConstraint(r.Body)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	// Make bucket.
	err = api.ObjectAPI.MakeBucket(bucketName, acl, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to create bucket "+bucketName)
		WriteErrorResponse(w, r, err)
		return
	}
	// Make sure to add Location information here only for bucket
	w.Header().Set("Location", GetLocation(r))
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) PutBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	var credential iam.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	acl, err := getAclFromHeader(r.Header)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	err = api.ObjectAPI.SetBucketAcl(bucket, acl, credential)
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

	bucket, err := api.ObjectAPI.GetBucketInfo(bucketName, credential)
	if err != nil {
		helper.ErrorIf(err, "Unable to fetch bucket info.")
		WriteErrorResponse(w, r, err)
		return
	}

	w.Header().Set("X-Amz-Acl", bucket.ACL.CannedAcl)
	w.Write(nil)
}

func (api ObjectAPIHandlers) PutBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	var credential iam.Credential
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

	cors, err := CorsFromXml(corsBuffer)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	err = api.ObjectAPI.SetBucketCors(bucketName, cors, credential)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) DeleteBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	var credential iam.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	err = api.ObjectAPI.DeleteBucketCors(bucketName, credential)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	WriteSuccessNoContent(w)
}

func (api ObjectAPIHandlers) GetBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	var credential iam.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	cors, err := api.ObjectAPI.GetBucketCors(bucketName, credential)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	corsBuffer, err := xml.Marshal(cors)
	if err != nil {
		helper.ErrorIf(err, "Failed to marshal CORS XML for bucket", bucketName)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
	WriteSuccessResponse(w, corsBuffer)
}

func (api ObjectAPIHandlers) GetBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	var credential iam.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	versioning, err := api.ObjectAPI.GetBucketVersioning(bucketName, credential)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	versioningBuffer, err := xml.Marshal(versioning)
	if err != nil {
		helper.ErrorIf(err, "Failed to marshal versioning XML for bucket", bucketName)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
	WriteSuccessResponse(w, versioningBuffer)
}

func (api ObjectAPIHandlers) PutBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	var credential iam.Credential
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
	err = api.ObjectAPI.SetBucketVersioning(bucketName, versioning, credential)
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

		if part.FileName() == "" {
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

// PostPolicyBucketHandler - POST policy upload
// ----------
// This implementation of the POST operation handles object creation with a specified
// signature policy in multipart/form-data

var ValidSuccessActionStatus = []string{"200", "201", "204"}

func (api ObjectAPIHandlers) PostPolicyBucketHandler(w http.ResponseWriter, r *http.Request) {
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

	var credential iam.Credential
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
	if !helper.StringInSlice(acl.CannedAcl, validCannedAcl) {
		WriteErrorResponse(w, r, ErrInvalidCannedAcl)
		return
	}

	sseRequest, err := parseSseHeader(headerfiedFormValues)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	result, err := api.ObjectAPI.PutObject(bucketName, objectName, credential, -1, fileBody,
		metadata, acl, sseRequest)
	if err != nil {
		helper.ErrorIf(err, "Unable to create object "+objectName)
		WriteErrorResponse(w, r, err)
		return
	}
	if result.Md5 != "" {
		w.Header().Set("ETag", "\""+result.Md5+"\"")
	}

	var redirect string
	redirect, _ = formValues["success_action_redirect"]
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
	status, _ = formValues["success_action_status"]
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

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (api ObjectAPIHandlers) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

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

	if _, err = api.ObjectAPI.GetBucketInfo(bucket, credential); err != nil {
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

	var credential iam.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	if err = api.ObjectAPI.DeleteBucket(bucket, credential); err != nil {
		helper.ErrorIf(err, "Unable to delete a bucket.")
		WriteErrorResponse(w, r, err)
		return
	}

	// Write success response.
	WriteSuccessNoContent(w)
}
