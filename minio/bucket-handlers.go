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

package minio

import (
	"bytes"
	"encoding/xml"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"

	"git.letv.cn/yig/yig/iam"
	"git.letv.cn/yig/yig/meta"
	. "git.letv.cn/yig/yig/minio/datatype"
	"git.letv.cn/yig/yig/signature"
	mux "github.com/gorilla/mux"
)

// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
func enforceBucketPolicy(action string, bucket string, reqURL *url.URL) (s3Error APIErrorCode) {
	// Read saved bucket policy.
	policy, err := readBucketPolicy(bucket)
	if err != nil {
		errorIf(err, "Unable read bucket policy.")
		switch err.(type) {
		case meta.BucketNotFound:
			return ErrNoSuchBucket
		case meta.BucketNameInvalid:
			return ErrInvalidBucketName
		default:
			// For any other error just return AccessDenied.
			return ErrAccessDenied
		}
	}
	// Parse the saved policy.
	bucketPolicy, err := parseBucketPolicy(policy)
	if err != nil {
		errorIf(err, "Unable to parse bucket policy.")
		return ErrAccessDenied
	}

	// Construct resource in 'arn:aws:s3:::examplebucket/object' format.
	resource := AWSResourcePrefix + strings.TrimPrefix(reqURL.Path, "/")

	// Get conditions for policy verification.
	conditions := make(map[string]string)
	for queryParam := range reqURL.Query() {
		conditions[queryParam] = reqURL.Query().Get(queryParam)
	}

	// Validate action, resource and conditions with current policy statements.
	if !bucketPolicyEvalStatements(action, resource, conditions, bucketPolicy.Statements) {
		return ErrAccessDenied
	}
	return ErrNone
}

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
func (api objectAPIHandlers) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	var credential iam.Credential
	var s3Error APIErrorCode
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		if s3Error := enforceBucketPolicy("s3:GetBucketLocation", bucket, r.URL); s3Error != ErrNone {
			WriteErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case signature.AuthTypeSignedV4, signature.AuthTypePresignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, s3Error = signature.IsReqAuthenticated(r); s3Error != ErrNone {
			WriteErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	if _, err := api.ObjectAPI.GetBucketInfo(bucket, credential); err != nil {
		errorIf(err, "Unable to fetch bucket info.")
		WriteErrorResponse(w, r, ToAPIErrorCode(err), r.URL.Path)
		return
	}

	// Generate response.
	encodedSuccessResponse := EncodeResponse(LocationResponse{})
	// Get current region.
	region := serverConfig.Region
	if region != "us-east-1" {
		encodedSuccessResponse = EncodeResponse(LocationResponse{
			Location: region,
		})
	}
	SetCommonHeaders(w) // Write headers.
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
func (api objectAPIHandlers) ListMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuAndPermissions.html
		if s3Error := enforceBucketPolicy("s3:ListBucketMultipartUploads", bucket, r.URL); s3Error != ErrNone {
			WriteErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4:
		if _, s3Error := signature.IsReqAuthenticated(r); s3Error != ErrNone {
			WriteErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	prefix, keyMarker, uploadIDMarker, delimiter, maxUploads, _ := getBucketMultipartResources(r.URL.Query())
	if maxUploads < 0 {
		WriteErrorResponse(w, r, ErrInvalidMaxUploads, r.URL.Path)
		return
	}
	if keyMarker != "" {
		// Marker not common with prefix is not implemented.
		if !strings.HasPrefix(keyMarker, prefix) {
			WriteErrorResponse(w, r, ErrNotImplemented, r.URL.Path)
			return
		}
	}

	listMultipartsInfo, err := api.ObjectAPI.ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter, maxUploads)
	if err != nil {
		errorIf(err, "Unable to list multipart uploads.")
		WriteErrorResponse(w, r, ToAPIErrorCode(err), r.URL.Path)
		return
	}
	// generate response
	response := GenerateListMultipartUploadsResponse(bucket, listMultipartsInfo)
	encodedSuccessResponse := EncodeResponse(response)
	// write headers.
	SetCommonHeaders(w)
	// write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
}

// ListObjectsHandler - GET Bucket (List Objects)
// -- -----------------------
// This implementation of the GET operation returns some or all (up to 1000)
// of the objects in a bucket. You can use the request parameters as selection
// criteria to return a subset of the objects in a bucket.
//
func (api objectAPIHandlers) ListObjectsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		if s3Error := enforceBucketPolicy("s3:ListBucket", bucket, r.URL); s3Error != ErrNone {
			WriteErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case signature.AuthTypeSignedV4, signature.AuthTypePresignedV4,
		signature.AuthTypeSignedV2, signature.AuthTypePresignedV2:
		if _, s3Error := signature.IsReqAuthenticated(r); s3Error != ErrNone {
			WriteErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}
	var prefix, marker, token, delimiter, startAfter string
	var maxkeys int
	var listV2 bool
	// TODO handle encoding type.
	if r.URL.Query().Get("list-type") == "2" {
		listV2 = true
		prefix, token, startAfter, delimiter, maxkeys, _ = getListObjectsV2Args(r.URL.Query())
		// For ListV2 "start-after" is considered only if "continuation-token" is empty.
		if token == "" {
			marker = startAfter
		} else {
			marker = token
		}
	} else {
		prefix, marker, delimiter, maxkeys, _ = getListObjectsV1Args(r.URL.Query())
	}
	if maxkeys <= 0 {
		WriteErrorResponse(w, r, ErrInvalidMaxKeys, r.URL.Path)
		return
	}
	// Verify if delimiter is anything other than '/', which we do not support.
	if delimiter != "" && delimiter != "/" {
		WriteErrorResponse(w, r, ErrNotImplemented, r.URL.Path)
		return
	}
	// If marker is set unescape.
	if marker != "" {
		// Marker not common with prefix is not implemented.
		if !strings.HasPrefix(marker, prefix) {
			WriteErrorResponse(w, r, ErrNotImplemented, r.URL.Path)
			return
		}
	}

	listObjectsInfo, err := api.ObjectAPI.ListObjects(bucket, prefix, marker, delimiter, maxkeys)

	if err == nil {
		var encodedSuccessResponse []byte
		// generate response
		if listV2 {
			response := GenerateListObjectsV2Response(bucket, prefix, token,
				startAfter, delimiter, maxkeys, listObjectsInfo)
			encodedSuccessResponse = EncodeResponse(response)
		} else {
			response := GenerateListObjectsResponse(bucket, prefix, marker,
				delimiter, maxkeys, listObjectsInfo)
			encodedSuccessResponse = EncodeResponse(response)
		}
		// Write headers
		SetCommonHeaders(w)
		// Write success response.
		WriteSuccessResponse(w, encodedSuccessResponse)
		return
	}
	errorIf(err, "Unable to list objects.")
	WriteErrorResponse(w, r, ToAPIErrorCode(err), r.URL.Path)
}

// ListBucketsHandler - GET Service
// -----------
// This implementation of the GET operation returns a list of all buckets
// owned by the authenticated sender of the request.
func (api objectAPIHandlers) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	// List buckets does not support bucket policies.
	var credential iam.Credential
	var s3Error APIErrorCode
	if credential, s3Error = signature.IsReqAuthenticated(r); s3Error != ErrNone {
		WriteErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	bucketsInfo, err := api.ObjectAPI.ListBuckets(credential)
	if err == nil {
		// generate response
		response := GenerateListBucketsResponse(bucketsInfo, credential)
		encodedSuccessResponse := EncodeResponse(response)
		// write headers
		SetCommonHeaders(w)
		// write response
		WriteSuccessResponse(w, encodedSuccessResponse)
		return
	}
	errorIf(err, "Unable to list buckets.")
	WriteErrorResponse(w, r, ToAPIErrorCode(err), r.URL.Path)
}

// DeleteMultipleObjectsHandler - deletes multiple objects.
func (api objectAPIHandlers) DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		if s3Error := enforceBucketPolicy("s3:DeleteObject", bucket, r.URL); s3Error != ErrNone {
			WriteErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4:
		if _, s3Error := signature.IsReqAuthenticated(r); s3Error != ErrNone {
			WriteErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	// Content-Length is required and should be non-zero
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if r.ContentLength <= 0 {
		WriteErrorResponse(w, r, ErrMissingContentLength, r.URL.Path)
		return
	}

	// Content-Md5 is requied should be set
	// http://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
	if _, ok := r.Header["Content-Md5"]; !ok {
		WriteErrorResponse(w, r, ErrMissingContentMD5, r.URL.Path)
		return
	}

	// Allocate incoming content length bytes.
	deleteXMLBytes := make([]byte, r.ContentLength)

	// Read incoming body XML bytes.
	if _, err := io.ReadFull(r.Body, deleteXMLBytes); err != nil {
		errorIf(err, "Unable to read HTTP body.")
		WriteErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}

	// Unmarshal list of keys to be deleted.
	deleteObjects := &DeleteObjectsRequest{}
	if err := xml.Unmarshal(deleteXMLBytes, deleteObjects); err != nil {
		errorIf(err, "Unable to unmarshal delete objects request XML.")
		WriteErrorResponse(w, r, ErrMalformedXML, r.URL.Path)
		return
	}

	var deleteErrors []DeleteError
	var deletedObjects []ObjectIdentifier
	// Loop through all the objects and delete them sequentially.
	for _, object := range deleteObjects.Objects {
		err := api.ObjectAPI.DeleteObject(bucket, object.ObjectName)
		if err == nil {
			deletedObjects = append(deletedObjects, ObjectIdentifier{
				ObjectName: object.ObjectName,
			})
		} else {
			errorIf(err, "Unable to delete object.")
			deleteErrors = append(deleteErrors, DeleteError{
				Code:    ErrorCodeResponse[ToAPIErrorCode(err)].Code,
				Message: ErrorCodeResponse[ToAPIErrorCode(err)].Description,
				Key:     object.ObjectName,
			})
		}
	}
	// Generate response
	response := GenerateMultiDeleteResponse(deleteObjects.Quiet, deletedObjects, deleteErrors)
	encodedSuccessResponse := EncodeResponse(response)
	// Write headers
	SetCommonHeaders(w)
	// Write success response.
	WriteSuccessResponse(w, encodedSuccessResponse)
}

// PutBucketHandler - PUT Bucket
// ----------
// This implementation of the PUT operation creates a new bucket for authenticated request
func (api objectAPIHandlers) PutBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	var credential iam.Credential
	var s3Error APIErrorCode
	if credential, s3Error = signature.IsReqAuthenticated(r); s3Error != ErrNone {
		WriteErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	// the location value in the request body should match the Region in serverConfig.
	// other values of location are not accepted.
	// make bucket fails in such cases.
	errCode := isValidLocationContraint(r.Body, serverConfig.Region)
	if errCode != ErrNone {
		WriteErrorResponse(w, r, errCode, r.URL.Path)
		return
	}
	// Make bucket.
	err := api.ObjectAPI.MakeBucket(bucket, credential)
	if err != nil {
		errorIf(err, "Unable to create a bucket.")
		WriteErrorResponse(w, r, ToAPIErrorCode(err), r.URL.Path)
		return
	}
	// Make sure to add Location information here only for bucket
	w.Header().Set("Location", GetLocation(r))
	WriteSuccessResponse(w, nil)
}

func extractHTTPFormValues(reader *multipart.Reader) (io.Reader, map[string]string, error) {
	/// HTML Form values
	formValues := make(map[string]string)
	filePart := new(bytes.Buffer)
	var err error
	for err == nil {
		var part *multipart.Part
		part, err = reader.NextPart()
		if part != nil {
			if part.FileName() == "" {
				var buffer []byte
				buffer, err = ioutil.ReadAll(part)
				if err != nil {
					return nil, nil, err
				}
				formValues[http.CanonicalHeaderKey(part.FormName())] = string(buffer)
			} else {
				if _, err = io.Copy(filePart, part); err != nil {
					return nil, nil, err
				}
			}
		}
	}
	return filePart, formValues, nil
}

// PostPolicyBucketHandler - POST policy
// ----------
// This implementation of the POST operation handles object creation with a specified
// signature policy in multipart/form-data
func (api objectAPIHandlers) PostPolicyBucketHandler(w http.ResponseWriter, r *http.Request) {
	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary files.
	reader, err := r.MultipartReader()
	if err != nil {
		errorIf(err, "Unable to initialize multipart reader.")
		WriteErrorResponse(w, r, ErrMalformedPOSTRequest, r.URL.Path)
		return
	}

	fileBody, formValues, err := extractHTTPFormValues(reader)
	if err != nil {
		errorIf(err, "Unable to parse form values.")
		WriteErrorResponse(w, r, ErrMalformedPOSTRequest, r.URL.Path)
		return
	}

	bucket := mux.Vars(r)["bucket"]

	postPolicyType := signature.GetPostPolicyType(formValues)
	var apiErr APIErrorCode
	switch postPolicyType {
	case signature.PostPolicyV2:
		_, apiErr = signature.DoesPolicySignatureMatchV2(formValues)
	case signature.PostPolicyV4:
		_, apiErr = signature.DoesPolicySignatureMatchV4(formValues)
		formValues["Bucket"] = bucket
	case signature.PostPolicyUnknown:
		WriteErrorResponse(w, r, ErrMalformedPOSTRequest, r.URL.Path)
		return
	}
	if apiErr != ErrNone {
		WriteErrorResponse(w, r, apiErr, r.URL.Path)
		return
	}

	if apiErr = signature.CheckPostPolicy(formValues, postPolicyType); apiErr != ErrNone {
		WriteErrorResponse(w, r, apiErr, r.URL.Path)
		return
	}

	// Save metadata.
	metadata := make(map[string]string)
	// Nothing to store right now.

	object := formValues["Key"]
	md5Sum, err := api.ObjectAPI.PutObject(bucket, object, -1, fileBody, metadata)
	if err != nil {
		errorIf(err, "Unable to create object.")
		WriteErrorResponse(w, r, ToAPIErrorCode(err), r.URL.Path)
		return
	}
	if md5Sum != "" {
		w.Header().Set("ETag", "\""+md5Sum+"\"")
	}
	encodedSuccessResponse := EncodeResponse(PostResponse{
		Location: GetObjectLocation(bucket, object), // TODO Full URL is preferred
		Bucket:   bucket,
		Key:      object,
		ETag:     md5Sum,
	})
	SetCommonHeaders(w)
	WriteSuccessResponse(w, encodedSuccessResponse)
}

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
func (api objectAPIHandlers) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	var credential iam.Credential
	var s3Error APIErrorCode
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypeAnonymous:
		// http://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
		if s3Error = enforceBucketPolicy("s3:ListBucket", bucket, r.URL); s3Error != ErrNone {
			WriteErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, s3Error = signature.IsReqAuthenticated(r); s3Error != ErrNone {
			WriteErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	if _, err := api.ObjectAPI.GetBucketInfo(bucket, credential); err != nil {
		errorIf(err, "Unable to fetch bucket info.")
		WriteErrorResponse(w, r, ToAPIErrorCode(err), r.URL.Path)
		return
	}
	WriteSuccessResponse(w, nil)
}

// DeleteBucketHandler - Delete bucket
func (api objectAPIHandlers) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	var credential iam.Credential
	var s3Error APIErrorCode
	if credential, s3Error = signature.IsReqAuthenticated(r); s3Error != ErrNone {
		WriteErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	if err := api.ObjectAPI.DeleteBucket(bucket, credential); err != nil {
		errorIf(err, "Unable to delete a bucket.")
		WriteErrorResponse(w, r, ToAPIErrorCode(err), r.URL.Path)
		return
	}

	// Delete bucket access policy, if present - ignore any errors.
	removeBucketPolicy(bucket)

	// Write success response.
	WriteSuccessNoContent(w)
}
