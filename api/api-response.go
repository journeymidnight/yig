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
	"net/http"
	"path"
	"time"

	"net/url"
	"strconv"

	. "github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	meta "github.com/journeymidnight/yig/meta/types"
)

const (
	timeFormatAMZ = "2006-01-02T15:04:05.000Z" // Reply date format
)

// DeleteObjectsResponse container for multiple object deletes.
type DeleteObjectsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult" json:"-"`

	// Collection of all deleted objects
	DeletedObjects []ObjectIdentifier `xml:"Deleted,omitempty"`

	// Collection of errors deleting certain objects.
	Errors []DeleteError `xml:"Error,omitempty"`
}

// getLocation get URL location.
func GetLocation(r *http.Request) string {
	return path.Clean(r.URL.Path) // Clean any trailing slashes.
}

// getObjectLocation gets the relative URL for an object
func GetObjectLocation(bucketName string, key string) string {
	return "/" + bucketName + "/" + key
}

// Takes an array of Bucket metadata information for serialization
// input: array of bucket metadata
// output: populated struct that can be serialized to match xml and json api spec output
func GenerateListBucketsResponse(buckets []meta.Bucket, credential common.Credential) ListBucketsResponse {
	var listBuckets []Bucket
	var data = ListBucketsResponse{}
	var owner = Owner{}

	owner.ID = credential.UserId
	owner.DisplayName = credential.DisplayName

	for _, bucket := range buckets {
		var listbucket = Bucket{}
		listbucket.Name = bucket.Name
		listbucket.CreationDate = bucket.CreateTime.Format(timeFormatAMZ)
		listBuckets = append(listBuckets, listbucket)
	}

	data.Owner = owner
	data.Buckets.Buckets = listBuckets

	return data
}

// generates an ListObjects response for the said bucket with other enumerated options.
func GenerateListObjectsResponse(bucketName string, request ListObjectsRequest,
	objectsInfo meta.ListObjectsInfo) (response ListObjectsResponse) {

	response.Contents = objectsInfo.Objects

	var prefixes []CommonPrefix
	for _, prefix := range objectsInfo.Prefixes {
		item := CommonPrefix{
			Prefix: prefix,
		}
		prefixes = append(prefixes, item)
	}
	response.CommonPrefixes = prefixes

	response.Delimiter = request.Delimiter
	response.EncodingType = request.EncodingType
	response.IsTruncated = objectsInfo.IsTruncated
	response.MaxKeys = request.MaxKeys
	response.Prefix = request.Prefix
	response.BucketName = bucketName

	if request.Version == 2 {
		response.KeyCount = len(response.Contents)

		response.ContinuationToken = request.ContinuationToken
		response.NextContinuationToken = objectsInfo.NextMarker
		response.StartAfter = request.StartAfter
	} else { // version 1
		response.Marker = request.Marker
		response.NextMarker = objectsInfo.NextMarker
	}

	if request.EncodingType != "" {
		response.Delimiter = url.QueryEscape(response.Delimiter)
		response.Prefix = url.QueryEscape(response.Prefix)
		response.StartAfter = url.QueryEscape(response.StartAfter)
		response.Marker = url.QueryEscape(response.Marker)
	}
	return
}

func GenerateVersionedListObjectResponse(bucketName string, request ListObjectsRequest,
	objectsInfo meta.VersionedListObjectsInfo) (response VersionedListObjectsResponse) {

	response.Contents = objectsInfo.Objects

	var prefixes []CommonPrefix
	for _, prefix := range objectsInfo.Prefixes {
		item := CommonPrefix{
			Prefix: prefix,
		}
		prefixes = append(prefixes, item)
	}
	response.CommonPrefixes = prefixes

	response.Delimiter = request.Delimiter
	response.EncodingType = request.EncodingType
	response.IsTruncated = objectsInfo.IsTruncated
	response.MaxKeys = request.MaxKeys
	response.KeyCount = len(response.Contents)
	response.Prefix = request.Prefix
	response.BucketName = bucketName

	response.KeyMarker = request.KeyMarker
	response.NextKeyMarker = objectsInfo.NextKeyMarker
	response.VersionIdMarker = request.VersionIdMarker
	response.NextVersionIdMarker = objectsInfo.NextVersionIdMarker

	if request.EncodingType != "" {
		response.KeyMarker = url.QueryEscape(response.KeyMarker)
		response.Delimiter = url.QueryEscape(response.Delimiter)
	}
	return
}

// GenerateCopyObjectResponse
func GenerateCopyObjectResponse(etag string, lastModified time.Time) CopyObjectResponse {
	return CopyObjectResponse{
		ETag:         "\"" + etag + "\"",
		LastModified: lastModified.UTC().Format(timeFormatAMZ),
	}
}

func GenerateCopyObjectPartResponse(etag string, lastModified time.Time) CopyObjectPartResponse {
	return CopyObjectPartResponse{
		LastModified: lastModified.UTC().Format(timeFormatAMZ),
		ETag:         "\"" + etag + "\"",
	}
}

// GenerateInitiateMultipartUploadResponse
func GenerateInitiateMultipartUploadResponse(bucket, key, uploadID string) InitiateMultipartUploadResponse {
	return InitiateMultipartUploadResponse{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}
}

// GenerateCompleteMultipartUploadResponse
func GenerateCompleteMultpartUploadResponse(bucket, key, location, etag string) CompleteMultipartUploadResponse {
	return CompleteMultipartUploadResponse{
		Location: location,
		Bucket:   bucket,
		Key:      key,
		ETag:     etag,
	}
}

// generate multi objects delete response.
func GenerateMultiDeleteResponse(quiet bool, deletedObjects []ObjectIdentifier, errs []DeleteError) DeleteObjectsResponse {
	deleteResp := DeleteObjectsResponse{}
	if !quiet {
		deleteResp.DeletedObjects = deletedObjects
	}
	deleteResp.Errors = errs
	return deleteResp
}

// WriteSuccessResponse write success headers and response if any.
func WriteSuccessResponse(w http.ResponseWriter, response []byte) {
	if response == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	//ResponseRecorder
	w.(*ResponseRecorder).status = http.StatusOK
	w.(*ResponseRecorder).size = int64(len(response))

	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.WriteHeader(http.StatusOK)
	w.Write(response)
	w.(http.Flusher).Flush()
}

// writeSuccessNoContent write success headers with http status 204
func WriteSuccessNoContent(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}

// writeErrorResponse write error headers
func WriteErrorResponse(w http.ResponseWriter, r *http.Request, err error) {
	WriteErrorResponseHeaders(w, err)
	WriteErrorResponseNoHeader(w, r, err, r.URL.Path)
}

func WriteErrorResponseWithResource(w http.ResponseWriter, r *http.Request, err error, resource string) {
	WriteErrorResponseHeaders(w, err)
	WriteErrorResponseNoHeader(w, r, err, resource)
}

func WriteErrorResponseHeaders(w http.ResponseWriter, err error) {
	var status int
	apiErrorCode, ok := err.(ApiError)
	if ok {
		status = apiErrorCode.HttpStatusCode()
	} else {
		status = http.StatusInternalServerError
	}
	helper.Logger.Println(5, "Response status code:", status, "err:", err)

	//ResponseRecorder
	w.(*ResponseRecorder).status = status

	w.WriteHeader(status)
}

func WriteErrorResponseNoHeader(w http.ResponseWriter, req *http.Request, err error, resource string) {
	// HEAD should have no body, do not attempt to write to it
	if req.Method == "HEAD" {
		return
	}

	// Generate error response.
	errorResponse := ApiErrorResponse{}
	apiErrorCode, ok := err.(ApiError)
	if ok {
		errorResponse.AwsErrorCode = apiErrorCode.AwsErrorCode()
		errorResponse.Message = apiErrorCode.Description()
	} else {
		errorResponse.AwsErrorCode = "InternalError"
		errorResponse.Message = "We encountered an internal error, please try again."
	}
	errorResponse.Resource = resource
	errorResponse.RequestId = requestIdFromContext(req.Context())
	errorResponse.HostId = helper.CONFIG.InstanceId

	encodedErrorResponse := EncodeResponse(errorResponse)

	//ResponseRecorder
	w.(*ResponseRecorder).size = int64(len(encodedErrorResponse))

	w.Write(encodedErrorResponse)
	w.(http.Flusher).Flush()
}

// APIErrorResponse - error response format
type ApiErrorResponse struct {
	XMLName      xml.Name `xml:"Error" json:"-"`
	AwsErrorCode string   `xml:"Code"`
	Message      string
	Key          string
	BucketName   string
	Resource     string
	RequestId    string
	HostId       string
}
