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
	MakeBucket(ctx context.Context, bucket string, acl datatype.Acl, credential common.Credential) error
	SetBucketLc(ctx context.Context, bucket string, config datatype.Lc,
		credential common.Credential) error
	GetBucketLc(ctx context.Context, bucket string, credential common.Credential) (datatype.Lc, error)
	DelBucketLc(ctx context.Context, bucket string, credential common.Credential) error
	SetBucketAcl(ctx context.Context, bucket string, policy datatype.AccessControlPolicy, acl datatype.Acl,
		credential common.Credential) error
	GetBucketAcl(ctx context.Context, bucket string, credential common.Credential) (datatype.AccessControlPolicyResponse, error)
	SetBucketCors(ctx context.Context, bucket string, cors datatype.Cors, credential common.Credential) error
	SetBucketVersioning(ctx context.Context, bucket string, versioning datatype.Versioning, credential common.Credential) error
	DeleteBucketCors(ctx context.Context, bucket string, credential common.Credential) error
	GetBucketVersioning(ctx context.Context, bucket string, credential common.Credential) (datatype.Versioning, error)
	GetBucketCors(ctx context.Context, bucket string, credential common.Credential) (datatype.Cors, error)
	GetBucket(ctx context.Context, bucketName string) (bucket *meta.Bucket, err error) // For INTERNAL USE ONLY
	GetBucketInfo(ctx context.Context, bucket string, credential common.Credential) (bucketInfo *meta.Bucket, err error)
	ListBuckets(ctx context.Context, credential common.Credential) (buckets []meta.Bucket, err error)
	DeleteBucket(ctx context.Context, bucket string, credential common.Credential) error
	ListObjects(ctx context.Context, credential common.Credential, bucket string,
		request datatype.ListObjectsRequest) (result meta.ListObjectsInfo, err error)
	ListVersionedObjects(ctx context.Context, credential common.Credential, bucket string,
		request datatype.ListObjectsRequest) (result meta.VersionedListObjectsInfo, err error)

	SetBucketPolicy(ctx context.Context, credential common.Credential, bucket string, policy policy.Policy) error
	// Policy operations
	GetBucketPolicy(ctx context.Context, credential common.Credential, bucket string) (policy.Policy, error)
	DeleteBucketPolicy(ctx context.Context, credential common.Credential, bucket string) error

	// Object operations.
	GetObject(ctx context.Context, object *meta.Object, startOffset int64, length int64, writer io.Writer,
		sse datatype.SseRequest) (err error)
	GetObjectInfo(ctx context.Context, bucket, object, version string, credential common.Credential) (objInfo *meta.Object,
		err error)
	PutObject(ctx context.Context, bucket, object string, credential common.Credential, size int64, data io.Reader,
		metadata map[string]string, acl datatype.Acl,
		sse datatype.SseRequest, storageClass meta.StorageClass) (result datatype.PutObjectResult, err error)
	AppendObject(ctx context.Context, bucket, object string, credential common.Credential, offset uint64, size int64, data io.Reader,
		metadata map[string]string, acl datatype.Acl,
		sse datatype.SseRequest, storageClass meta.StorageClass, objInfo *meta.Object) (result datatype.AppendObjectResult, err error)

	CopyObject(ctx context.Context, targetObject *meta.Object, source io.Reader, credential common.Credential,
		sse datatype.SseRequest) (result datatype.PutObjectResult, err error)
	UpdateObjectAttrs(ctx context.Context, targetObject *meta.Object, credential common.Credential) (result datatype.PutObjectResult, err error)
	SetObjectAcl(ctx context.Context, bucket string, object string, version string, policy datatype.AccessControlPolicy,
		acl datatype.Acl, credential common.Credential) error
	GetObjectAcl(ctx context.Context, bucket string, object string, version string, credential common.Credential) (
		policy datatype.AccessControlPolicyResponse, err error)
	DeleteObject(ctx context.Context, bucket, object, version string, credential common.Credential) (datatype.DeleteObjectResult,
		error)

	// Multipart operations.
	ListMultipartUploads(ctx context.Context, credential common.Credential, bucket string,
		request datatype.ListUploadsRequest) (result datatype.ListMultipartUploadsResponse, err error)
	NewMultipartUpload(ctx context.Context, credential common.Credential, bucket, object string,
		metadata map[string]string, acl datatype.Acl,
		sse datatype.SseRequest, storageClass meta.StorageClass) (uploadID string, err error)
	PutObjectPart(ctx context.Context, bucket, object string, credential common.Credential, uploadID string, partID int,
		size int64, data io.Reader, md5Hex string,
		sse datatype.SseRequest) (result datatype.PutObjectPartResult, err error)
	CopyObjectPart(ctx context.Context, bucketName, objectName, uploadId string, partId int, size int64, data io.Reader,
		credential common.Credential, sse datatype.SseRequest) (result datatype.PutObjectResult,
		err error)
	ListObjectParts(ctx context.Context, credential common.Credential, bucket, object string,
		request datatype.ListPartsRequest) (result datatype.ListPartsResponse, err error)
	AbortMultipartUpload(ctx context.Context, credential common.Credential, bucket, object, uploadID string) error
	CompleteMultipartUpload(ctx context.Context, credential common.Credential, bucket, object, uploadID string,
		uploadedParts []meta.CompletePart) (result datatype.CompleteMultipartResult, err error)
}
