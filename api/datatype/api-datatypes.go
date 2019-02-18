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

package datatype

import (
	"encoding/xml"
	"time"
)

const (
	MaxObjectList  = 1000 // Limit number of objects in a listObjectsResponse.
	MaxUploadsList = 1000 // Limit number of uploads in a listUploadsResponse.
	MaxPartsList   = 1000 // Limit number of parts in a listPartsResponse.
)

// LocationResponse - format for location response.
type LocationResponse struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint" json:"-"`
	Location string   `xml:",chardata"`
}

type ListObjectsResponse struct {
	XMLName xml.Name `xml:"ListBucketResult"`

	CommonPrefixes []CommonPrefix
	Delimiter      string
	EncodingType   string `xml:"Encoding-Type,omitempty"`
	IsTruncated    bool
	MaxKeys        int
	KeyCount       int `xml:",omitempty"`
	Prefix         string
	BucketName     string `xml:"Name"`

	// v1 specific
	Marker     string
	NextMarker string `xml:",omitempty"`

	// v2 specific
	ContinuationToken     string `xml:",omitempty"`
	NextContinuationToken string `xml:",omitempty"`
	StartAfter            string `xml:",omitempty"`

	Contents []Object
}

type VersionedListObjectsResponse struct {
	XMLName xml.Name `xml:"ListVersionsResult"`

	Contents            []VersionedObject
	CommonPrefixes      []CommonPrefix
	Delimiter           string
	EncodingType        string `xml:"Encoding-Type,omitempty"`
	IsTruncated         bool
	MaxKeys             int
	KeyCount            int
	Prefix              string
	BucketName          string `xml:"Name"`
	KeyMarker           string
	NextKeyMarker       string
	VersionIdMarker     string
	NextVersionIdMarker string
}

type ListObjectsRequest struct {
	Versioned    bool // should return versioned objects?
	Version      int  // Currently 1 or 2
	Delimiter    string
	EncodingType string
	MaxKeys      int
	Prefix       string

	// v1 specific
	Marker string

	// v2 specific
	ContinuationToken string
	StartAfter        string
	FetchOwner        bool

	// versioned specific
	KeyMarker       string
	VersionIdMarker string
}

type ListUploadsRequest struct {
	Delimiter      string
	EncodingType   string
	MaxUploads     int
	KeyMarker      string
	Prefix         string
	UploadIdMarker string
}

type ListPartsRequest struct {
	EncodingType     string
	UploadId         string
	MaxParts         int
	PartNumberMarker int
}

// Part container for part metadata.
type Part struct {
	PartNumber   int
	ETag         string
	LastModified string
	Size         int64
}

// ListPartsResponse - format for list parts response.
type ListPartsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListPartsResult" json:"-"`

	Bucket       string
	Key          string
	UploadId     string
	EncodingType string `xml:"Encoding-Type,omitempty"`

	Initiator Initiator
	Owner     Owner

	// The class of storage used to store the object.
	StorageClass string

	PartNumberMarker     int
	NextPartNumberMarker int
	MaxParts             int
	IsTruncated          bool

	// List of parts.
	Parts []Part `xml:"Part"`
}

// ListMultipartUploadsResponse - format for list multipart uploads response.
type ListMultipartUploadsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListMultipartUploadsResult" json:"-"`

	Bucket             string
	KeyMarker          string
	UploadIdMarker     string
	NextKeyMarker      string
	NextUploadIdMarker string
	EncodingType       string `xml:"Encoding-Type,omitempty"`
	MaxUploads         int
	IsTruncated        bool
	Uploads            []Upload `xml:"Upload"`
	Prefix             string
	Delimiter          string
	CommonPrefixes     []CommonPrefix
}

// ListBucketsResponse - format for list buckets response
type ListBucketsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult" json:"-"`
	Owner   Owner
	// Container for one or more buckets.
	Buckets struct {
		Buckets []Bucket `xml:"Bucket"`
	} // Buckets are nested
}

// Upload container for in progress multipart upload
type Upload struct {
	Key          string
	UploadId     string
	Initiator    Initiator
	Owner        Owner
	StorageClass string
	Initiated    string // time string of format "2006-01-02T15:04:05.000Z"
}

// CommonPrefix container for prefix response in ListObjectsResponse
type CommonPrefix struct {
	XMLName xml.Name `xml:"CommonPrefixes"`
	Prefix  string
}

// Bucket container for bucket metadata
type Bucket struct {
	Name         string
	CreationDate string // time string of format "2006-01-02T15:04:05.000Z"
}

// Object container for object metadata
type Object struct {
	XMLName      xml.Name `xml:"Contents"`
	Key          string
	LastModified string // time string of format "2006-01-02T15:04:05.000Z"
	ETag         string
	Size         int64

	Owner Owner

	// The class of storage used to store the object.
	StorageClass string
}

type VersionedObject struct {
	XMLName   xml.Name
	Key       string
	VersionId string
	// TODO: IsLatest
	// IsLatest     bool
	LastModified string // time string of format "2006-01-02T15:04:05.000Z"
	ETag         string
	Size         int64
	StorageClass string
	Owner        Owner
}

// CopyObjectResponse container returns ETag and LastModified of the
// successfully copied object
type CopyObjectResponse struct {
	XMLName      xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CopyObjectResult" json:"-"`
	ETag         string
	LastModified string // time string of format "2006-01-02T15:04:05.000Z"
}

type CopyObjectPartResponse struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	LastModified string
	ETag         string
}

// Initiator inherit from Owner struct, fields are same
type Initiator Owner

// Owner - bucket owner/principal
type Owner struct {
	ID          string
	DisplayName string
}

// InitiateMultipartUploadResponse container for InitiateMultiPartUpload response, provides uploadID to start MultiPart upload
type InitiateMultipartUploadResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult" json:"-"`

	Bucket   string
	Key      string
	UploadID string `xml:"UploadId"`
}

// CompleteMultipartUploadResponse container for completed multipart upload response
type CompleteMultipartUploadResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult" json:"-"`

	Location string
	Bucket   string
	Key      string
	ETag     string
}

// PostResponse container for completed post upload response
type PostResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ PostResponse" json:"-"`

	Location string
	Bucket   string
	Key      string
	ETag     string
}

// DeleteError structure.
type DeleteError struct {
	Code      string
	Message   string
	Key       string
	VersionId string `xml:",omitempty"`
}

// ObjectIdentifier carries key name for the object to delete.
type ObjectIdentifier struct {
	ObjectName            string `xml:"Key"`
	VersionId             string `xml:",omitempty"`
	DeleteMarker          bool   `xml:",omitempty"`
	DeleteMarkerVersionId string `xml:",omitempty"`
}

// createBucketConfiguration container for bucket configuration request from client.
// Used for parsing the location from the request body for MakeBucket.
type CreateBucketLocationConfiguration struct {
	XMLName  xml.Name `xml:"CreateBucketConfiguration" json:"-"`
	Location string   `xml:"LocationConstraint"`
}

// DeleteObjectsRequest - xml carrying the object key names which needs to be deleted.
type DeleteObjectsRequest struct {
	XMLName xml.Name `xml:"Delete"`
	// Element to enable quiet mode for the request
	Quiet bool
	// List of objects to be deleted
	Objects []ObjectIdentifier `xml:"Object"`
}

type PutObjectResult struct {
	Md5          string
	VersionId    string
	LastModified time.Time
}

type AppendObjectResult struct {
	PutObjectResult
	NextPosition int64
}

type DeleteObjectResult struct {
	DeleteMarker bool
	VersionId    string
}

type PutObjectPartResult struct {
	ETag                    string
	SseType                 string
	SseAwsKmsKeyIdBase64    string
	SseCustomerAlgorithm    string
	SseCustomerKeyMd5Base64 string
}

type CompleteMultipartResult struct {
	ETag                    string
	VersionId               string
	SseType                 string
	SseAwsKmsKeyIdBase64    string
	SseCustomerAlgorithm    string
	SseCustomerKeyMd5Base64 string
}

type SseRequest struct {
	// type of Server Side Encryption, could be "SSE-KMS", "SSE-S3", "SSE-C"(custom), or ""(none),
	// KMS is not implemented yet
	Type string

	// AWS-managed specific(KMS and S3)
	SseAwsKmsKeyId string
	SseContext     string

	// customer-provided specific(SSE-C)
	SseCustomerAlgorithm string
	SseCustomerKey       []byte

	// keys for copy
	CopySourceSseCustomerAlgorithm string
	CopySourceSseCustomerKey       []byte
}
