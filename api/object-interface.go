/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	"io"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/policy"
	"github.com/journeymidnight/yig/iam/common"
	meta "github.com/journeymidnight/yig/meta/types"
)

// ObjectLayer implements primitives for object API layer.
type ObjectLayer interface {
	// Bucket operations.
	MakeBucket(bucket string, acl datatype.Acl, credential common.Credential) error
	SetBucketLc(bucket string, config datatype.Lc,
		credential common.Credential) error
	GetBucketLc(bucket string, credential common.Credential) (datatype.Lc, error)
	DelBucketLc(bucket string, credential common.Credential) error
	SetBucketAcl(bucket string, policy datatype.AccessControlPolicy, acl datatype.Acl,
		credential common.Credential) error
	GetBucketAcl(bucket string, credential common.Credential) (datatype.AccessControlPolicyResponse, error)
	SetBucketCors(bucket string, cors datatype.Cors, credential common.Credential) error
	SetBucketVersioning(bucket string, versioning datatype.Versioning, credential common.Credential) error
	DeleteBucketCors(bucket string, credential common.Credential) error
	GetBucketVersioning(bucket string, credential common.Credential) (datatype.Versioning, error)
	GetBucketCors(bucket string, credential common.Credential) (datatype.Cors, error)
	GetBucket(bucketName string) (bucket *meta.Bucket, err error) // For INTERNAL USE ONLY
	GetBucketInfo(bucket string, credential common.Credential) (bucketInfo *meta.Bucket, err error)
	ListBuckets(credential common.Credential) (buckets []meta.Bucket, err error)
	DeleteBucket(bucket string, credential common.Credential) error
	ListObjects(credential common.Credential, bucket string,
		request datatype.ListObjectsRequest) (result meta.ListObjectsInfo, err error)
	ListVersionedObjects(credential common.Credential, bucket string,
		request datatype.ListObjectsRequest) (result meta.VersionedListObjectsInfo, err error)

	SetBucketPolicy(credential common.Credential, bucket string, policy policy.Policy) error
	// Policy operations
	GetBucketPolicy(credential common.Credential, bucket string) (policy.Policy, error)
	DeleteBucketPolicy(credential common.Credential, bucket string) error

	// Object operations.
	GetObject(object *meta.Object, startOffset int64, length int64, writer io.Writer,
		sse datatype.SseRequest) (err error)
	GetObjectInfo(bucket, object, version string, credential common.Credential) (objInfo *meta.Object,
		err error)
	PutObject(bucket, object string, credential common.Credential, size int64, data io.Reader,
		metadata map[string]string, acl datatype.Acl,
		sse datatype.SseRequest, storageClass meta.StorageClass) (result datatype.PutObjectResult, err error)
	AppendObject(bucket, object string, credential common.Credential, offset uint64, size int64, data io.Reader,
		metadata map[string]string, acl datatype.Acl,
		sse datatype.SseRequest, storageClass meta.StorageClass, objInfo *meta.Object) (result datatype.AppendObjectResult, err error)

	CopyObject(targetObject *meta.Object, source io.Reader, credential common.Credential,
		sse datatype.SseRequest) (result datatype.PutObjectResult, err error)
	UpdateObjectAttrs(targetObject *meta.Object, credential common.Credential) (result datatype.PutObjectResult, err error)
	SetObjectAcl(bucket string, object string, version string, policy datatype.AccessControlPolicy,
		acl datatype.Acl, credential common.Credential) error
	GetObjectAcl(bucket string, object string, version string, credential common.Credential) (
		policy datatype.AccessControlPolicyResponse, err error)
	DeleteObject(bucket, object, version string, credential common.Credential) (datatype.DeleteObjectResult,
		error)

	// Multipart operations.
	ListMultipartUploads(credential common.Credential, bucket string,
		request datatype.ListUploadsRequest) (result datatype.ListMultipartUploadsResponse, err error)
	NewMultipartUpload(credential common.Credential, bucket, object string,
		metadata map[string]string, acl datatype.Acl,
		sse datatype.SseRequest, storageClass meta.StorageClass) (uploadID string, err error)
	PutObjectPart(bucket, object string, credential common.Credential, uploadID string, partID int,
		size int64, data io.Reader, md5Hex string,
		sse datatype.SseRequest) (result datatype.PutObjectPartResult, err error)
	CopyObjectPart(bucketName, objectName, uploadId string, partId int, size int64, data io.Reader,
		credential common.Credential, sse datatype.SseRequest) (result datatype.PutObjectResult,
		err error)
	ListObjectParts(credential common.Credential, bucket, object string,
		request datatype.ListPartsRequest) (result datatype.ListPartsResponse, err error)
	AbortMultipartUpload(credential common.Credential, bucket, object, uploadID string) error
	CompleteMultipartUpload(credential common.Credential, bucket, object, uploadID string,
		uploadedParts []meta.CompletePart) (result datatype.CompleteMultipartResult, err error)
}
