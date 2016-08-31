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

	. "git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/iam"
	"git.letv.cn/yig/yig/meta"
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

// takes an array of Bucketmetadata information for serialization
// input:
// array of bucket metadata
//
// output:
// populated struct that can be serialized to match xml and json api spec output
func GenerateListBucketsResponse(buckets []meta.Bucket, credential iam.Credential) ListBucketsResponse {
	var listbuckets []Bucket
	var data = ListBucketsResponse{}
	var owner = Owner{}

	owner.ID = credential.UserId
	owner.DisplayName = credential.DisplayName

	for _, bucket := range buckets {
		var listbucket = Bucket{}
		listbucket.Name = bucket.Name
		listbucket.CreationDate = bucket.CreateTime.Format(timeFormatAMZ)
		listbuckets = append(listbuckets, listbucket)
	}

	data.Owner = owner
	data.Buckets.Buckets = listbuckets

	return data
}

// generates an ListObjects response for the said bucket with other enumerated options.
func GenerateListObjectsResponse(bucketName string, request ListObjectsRequest,
	objectList meta.ListObjectsInfo) (response ListObjectsResponse, err error) {

	response.Contents = objectList.Objects

	var prefixes []CommonPrefix
	for _, prefix := range objectList.Prefixes {
		item := CommonPrefix{
			Prefix: prefix,
		}
		prefixes = append(prefixes, item)
	}
	response.CommonPrefixes = prefixes

	response.Delimiter = request.Delimiter
	response.EncodingType = request.EncodingType
	response.IsTruncated = objectList.IsTruncated
	response.MaxKeys = request.MaxKeys
	response.KeyCount = len(response.Contents)
	response.Prefix = request.Prefix
	response.BucketName = bucketName

	if request.Version == 2 {
		response.ContinuationToken = request.ContinuationToken
		response.NextContinuationToken = objectList.NextMarker
		response.StartAfter = request.StartAfter
	} else { // version 1
		response.Marker = request.Marker
		response.NextMarker = objectList.NextMarker
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

// GenerateListPartsResult
func GenerateListPartsResponse(partsInfo meta.ListPartsInfo) ListPartsResponse {
	// TODO - support EncodingType in xml decoding
	listPartsResponse := ListPartsResponse{}
	listPartsResponse.Bucket = partsInfo.Bucket
	listPartsResponse.Key = partsInfo.Object
	listPartsResponse.UploadID = partsInfo.UploadID
	listPartsResponse.StorageClass = partsInfo.StorageClass

	// FIXME merge data structure ListPartsResponse and ListPartsInfo
	initiator, _ := iam.GetCredentialByUserId(partsInfo.InitiatorId)
	listPartsResponse.Initiator.ID = initiator.UserId
	listPartsResponse.Initiator.DisplayName = initiator.DisplayName
	owner, _ := iam.GetCredentialByUserId(partsInfo.OwnerId)
	listPartsResponse.Owner.ID = owner.UserId
	listPartsResponse.Owner.DisplayName = owner.DisplayName

	listPartsResponse.MaxParts = partsInfo.MaxParts
	listPartsResponse.PartNumberMarker = partsInfo.PartNumberMarker
	listPartsResponse.IsTruncated = partsInfo.IsTruncated
	listPartsResponse.NextPartNumberMarker = partsInfo.NextPartNumberMarker

	listPartsResponse.Parts = make([]Part, len(partsInfo.Parts))
	for index, part := range partsInfo.Parts {
		newPart := Part{}
		newPart.PartNumber = part.PartNumber
		newPart.ETag = "\"" + part.Etag + "\""
		newPart.Size = part.Size
		newPart.LastModified = part.LastModified.UTC().Format(timeFormatAMZ)
		listPartsResponse.Parts[index] = newPart
	}
	return listPartsResponse
}

// generateListMultipartUploadsResponse
func GenerateListMultipartUploadsResponse(bucket string, multipartsInfo meta.ListMultipartsInfo) ListMultipartUploadsResponse {
	listMultipartUploadsResponse := ListMultipartUploadsResponse{}
	listMultipartUploadsResponse.Bucket = bucket
	listMultipartUploadsResponse.Delimiter = multipartsInfo.Delimiter
	listMultipartUploadsResponse.IsTruncated = multipartsInfo.IsTruncated
	listMultipartUploadsResponse.EncodingType = multipartsInfo.EncodingType
	listMultipartUploadsResponse.Prefix = multipartsInfo.Prefix
	listMultipartUploadsResponse.KeyMarker = multipartsInfo.KeyMarker
	listMultipartUploadsResponse.NextKeyMarker = multipartsInfo.NextKeyMarker
	listMultipartUploadsResponse.MaxUploads = multipartsInfo.MaxUploads
	listMultipartUploadsResponse.NextUploadIDMarker = multipartsInfo.NextUploadIDMarker
	listMultipartUploadsResponse.UploadIDMarker = multipartsInfo.UploadIDMarker
	listMultipartUploadsResponse.CommonPrefixes = make([]CommonPrefix, len(multipartsInfo.CommonPrefixes))
	for index, commonPrefix := range multipartsInfo.CommonPrefixes {
		listMultipartUploadsResponse.CommonPrefixes[index] = CommonPrefix{
			Prefix: commonPrefix,
		}
	}
	listMultipartUploadsResponse.Uploads = make([]Upload, len(multipartsInfo.Uploads))
	for index, upload := range multipartsInfo.Uploads {
		newUpload := Upload{}
		newUpload.UploadID = upload.UploadID
		newUpload.Key = upload.Object
		newUpload.Initiated = upload.Initiated.UTC().Format(timeFormatAMZ)
		listMultipartUploadsResponse.Uploads[index] = newUpload
	}
	return listMultipartUploadsResponse
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
	SetCommonHeaders(w)
	if response == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.Write(response)
	w.(http.Flusher).Flush()
}

// writeSuccessNoContent write success headers with http status 204
func WriteSuccessNoContent(w http.ResponseWriter) {
	SetCommonHeaders(w)
	w.WriteHeader(http.StatusNoContent)
}

// writeErrorRespone write error headers
func WriteErrorResponse(w http.ResponseWriter, req *http.Request, err error, resource string) {
	WriteErrorResponseHeaders(w, req, err, resource)
	WriteErrorResponseNoHeader(w, req, err, resource)
}

func WriteErrorResponseHeaders(w http.ResponseWriter, req *http.Request, err error, resource string) {
	// set common headers
	SetCommonHeaders(w)

	apiErrorCode, ok := err.(ApiError)
	if ok {
		w.WriteHeader(apiErrorCode.HttpStatusCode())
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func WriteErrorResponseNoHeader(w http.ResponseWriter, req *http.Request, err error, resource string) {
	// Generate error response.
	errorResponse := GetAPIErrorResponse(err, resource)
	encodedErrorResponse := EncodeResponse(errorResponse)
	// HEAD should have no body, do not attempt to write to it
	if req.Method != "HEAD" {
		// write error body
		w.Write(encodedErrorResponse)
		w.(http.Flusher).Flush()
	}
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

// GetErrorResponse gets in standard error and resource value and
// provides a encodable populated response values
func GetAPIErrorResponse(err error, resource string) ApiErrorResponse {
	var data = ApiErrorResponse{}
	apiErrorCode, ok := err.(ApiError)
	if ok {
		data.AwsErrorCode = apiErrorCode.AwsErrorCode()
		data.Message = apiErrorCode.Description()
	} else {
		data.AwsErrorCode = "InternalError"
		data.Message = "We encountered an internal error, please try again."
	}
	if resource != "" {
		data.Resource = resource
	}
	// TODO implement this in future
	data.RequestId = "3L137"
	data.HostId = "3L137"

	return data
}
