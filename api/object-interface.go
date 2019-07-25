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
	"context"
	"io"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/policy"
	"github.com/journeymidnight/yig/iam/common"
	meta "github.com/journeymidnight/yig/meta/types"
)

// ObjectLayer implements primitives for object API layer.
type ObjectLayer interface {
	// Bucket operations.
	MakeBucket(bucket string, acl datatype.Acl, credential common.Credential, ctx context.Context) error
	SetBucketLc(bucket string, config datatype.Lc,
		credential common.Credential, ctx context.Context) error
	GetBucketLc(bucket string, credential common.Credential, ctx context.Context) (datatype.Lc, error)
	DelBucketLc(bucket string, credential common.Credential, ctx context.Context) error
	SetBucketAcl(bucket string, policy datatype.AccessControlPolicy, acl datatype.Acl,
		credential common.Credential, ctx context.Context) error
	GetBucketAcl(bucket string, credential common.Credential, ctx context.Context) (datatype.AccessControlPolicyResponse, error)
	SetBucketCors(bucket string, cors datatype.Cors, credential common.Credential, ctx context.Context) error
	SetBucketVersioning(bucket string, versioning datatype.Versioning, credential common.Credential, ctx context.Context) error
	DeleteBucketCors(bucket string, credential common.Credential, ctx context.Context) error
	GetBucketVersioning(bucket string, credential common.Credential, ctx context.Context) (datatype.Versioning, error)
	GetBucketCors(bucket string, credential common.Credential, ctx context.Context) (datatype.Cors, error)
	GetBucket(bucketName string, ctx context.Context) (bucket *meta.Bucket, err error) // For INTERNAL USE ONLY
	GetBucketInfo(bucket string, credential common.Credential, ctx context.Context) (bucketInfo *meta.Bucket, err error)
	ListBuckets(credential common.Credential, ctx context.Context) (buckets []meta.Bucket, err error)
	DeleteBucket(bucket string, credential common.Credential, ctx context.Context) error
	ListObjects(credential common.Credential, bucket string,
		request datatype.ListObjectsRequest, ctx context.Context) (result meta.ListObjectsInfo, err error)
	ListVersionedObjects(credential common.Credential, bucket string,
		request datatype.ListObjectsRequest, ctx context.Context) (result meta.VersionedListObjectsInfo, err error)

	SetBucketPolicy(credential common.Credential, bucket string, policy policy.Policy, ctx context.Context) error
	// Policy operations
	GetBucketPolicy(credential common.Credential, bucket string, ctx context.Context) (policy.Policy, error)
	DeleteBucketPolicy(credential common.Credential, bucket string, ctx context.Context) error

	// Object operations.
	GetObject(object *meta.Object, startOffset int64, length int64, writer io.Writer,
		sse datatype.SseRequest, ctx context.Context) (err error)
	GetObjectInfo(bucket, object, version string, credential common.Credential, ctx context.Context) (objInfo *meta.Object,
		err error)
	PutObject(bucket, object string, credential common.Credential, size int64, data io.Reader,
		metadata map[string]string, acl datatype.Acl,
		sse datatype.SseRequest, storageClass meta.StorageClass, ctx context.Context) (result datatype.PutObjectResult, err error)
	AppendObject(bucket, object string, credential common.Credential, offset uint64, size int64, data io.Reader,
		metadata map[string]string, acl datatype.Acl,
		sse datatype.SseRequest, storageClass meta.StorageClass, objInfo *meta.Object, ctx context.Context) (result datatype.AppendObjectResult, err error)

	CopyObject(targetObject *meta.Object, source io.Reader, credential common.Credential,
		sse datatype.SseRequest, ctx context.Context) (result datatype.PutObjectResult, err error)
	UpdateObjectAttrs(targetObject *meta.Object, credential common.Credential, ctx context.Context) (result datatype.PutObjectResult, err error)
	SetObjectAcl(bucket string, object string, version string, policy datatype.AccessControlPolicy,
		acl datatype.Acl, credential common.Credential, ctx context.Context) error
	GetObjectAcl(bucket string, object string, version string, credential common.Credential, ctx context.Context) (
		policy datatype.AccessControlPolicyResponse, err error)
	DeleteObject(bucket, object, version string, credential common.Credential, ctx context.Context) (datatype.DeleteObjectResult,
		error)

	// Multipart operations.
	ListMultipartUploads(credential common.Credential, bucket string,
		request datatype.ListUploadsRequest, ctx context.Context) (result datatype.ListMultipartUploadsResponse, err error)
	NewMultipartUpload(credential common.Credential, bucket, object string,
		metadata map[string]string, acl datatype.Acl,
		sse datatype.SseRequest, storageClass meta.StorageClass, ctx context.Context) (uploadID string, err error)
	PutObjectPart(bucket, object string, credential common.Credential, uploadID string, partID int,
		size int64, data io.Reader, md5Hex string,
		sse datatype.SseRequest, ctx context.Context) (result datatype.PutObjectPartResult, err error)
	CopyObjectPart(bucketName, objectName, uploadId string, partId int, size int64, data io.Reader,
		credential common.Credential, sse datatype.SseRequest, ctx context.Context) (result datatype.PutObjectResult,
		err error)
	ListObjectParts(credential common.Credential, bucket, object string,
		request datatype.ListPartsRequest, ctx context.Context) (result datatype.ListPartsResponse, err error)
	AbortMultipartUpload(credential common.Credential, bucket, object, uploadID string, ctx context.Context) error
	CompleteMultipartUpload(credential common.Credential, bucket, object, uploadID string,
		uploadedParts []meta.CompletePart, ctx context.Context) (result datatype.CompleteMultipartResult, err error)
}
