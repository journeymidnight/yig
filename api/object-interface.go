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
	. "github.com/journeymidnight/yig/context"
	"github.com/journeymidnight/yig/iam/common"
	meta "github.com/journeymidnight/yig/meta/types"
)

// ObjectLayer implements primitives for object API layer.
type ObjectLayer interface {
	// Bucket operations.

	SetBucketLogging(reqCtx RequestContext, config datatype.BucketLoggingStatus,
		credential common.Credential) error
	GetBucketLogging(reqCtx RequestContext, credential common.Credential) (datatype.BucketLoggingStatus, error)
	MakeBucket(reqCtx RequestContext, acl datatype.Acl, credential common.Credential) error
	SetBucketLifecycle(reqCtx RequestContext, config datatype.Lifecycle,
		credential common.Credential) error
	GetBucketLifecycle(reqCtx RequestContext, credential common.Credential) (datatype.Lifecycle, error)
	DelBucketLifecycle(reqCtx RequestContext, credential common.Credential) error
	SetBucketAcl(reqCtx RequestContext, policy datatype.AccessControlPolicy, acl datatype.Acl,
		credential common.Credential) error
	GetBucketAcl(reqCtx RequestContext, credential common.Credential) (datatype.AccessControlPolicyResponse, error)
	SetBucketCors(reqCtx RequestContext, cors datatype.Cors, credential common.Credential) error
	SetBucketVersioning(reqCtx RequestContext, versioning datatype.Versioning, credential common.Credential) error
	DeleteBucketCors(reqCtx RequestContext, credential common.Credential) error
	GetBucketVersioning(reqCtx RequestContext, credential common.Credential) (datatype.Versioning, error)
	GetBucketCors(reqCtx RequestContext, credential common.Credential) (datatype.Cors, error)
	GetBucket(bucketName string) (bucket *meta.Bucket, err error) // For INTERNAL USE ONLY
	GetBucketInfo(reqCtx RequestContext, credential common.Credential) (bucketInfo *meta.Bucket, err error)
	GetBucketInfoByCtx(ctx RequestContext, credential common.Credential) (bucket *meta.Bucket, err error)
	ListBuckets(credential common.Credential) (buckets []meta.Bucket, err error)
	DeleteBucket(reqCtx RequestContext, credential common.Credential) error
	ListObjects(reqCtx RequestContext, credential common.Credential, request datatype.ListObjectsRequest) (
		result meta.ListObjectsInfo, err error)
	ListVersionedObjects(reqCtx RequestContext, credential common.Credential, request datatype.ListObjectsRequest) (
		result meta.VersionedListObjectsInfo, err error)

	SetBucketPolicy(credential common.Credential, bucket string, policy policy.Policy) error
	// Policy operations
	GetBucketPolicy(credential common.Credential, bucket string) (policy.Policy, error)
	DeleteBucketPolicy(credential common.Credential, bucket string) error

	// Website operations
	SetBucketWebsite(bucket *meta.Bucket, config datatype.WebsiteConfiguration) error
	GetBucketWebsite(bucket string) (datatype.WebsiteConfiguration, error)
	DeleteBucketWebsite(bucket *meta.Bucket) error

	// Encryption operations
	SetBucketEncryption(bucket *meta.Bucket, config datatype.EncryptionConfiguration) error
	GetBucketEncryption(bucket string) (datatype.EncryptionConfiguration, error)
	DeleteBucketEncryption(bucket *meta.Bucket) error
	CheckBucketEncryption(bucket *meta.Bucket) (*datatype.ApplyServerSideEncryptionByDefault, bool)

	// Object operations.
	GetObject(object *meta.Object, startOffset int64, length int64, writer io.Writer,
		sse datatype.SseRequest) (err error)
	GetObjectInfo(bucket, object, version string, credential common.Credential) (objInfo *meta.Object, err error)
	GetObjectInfoByCtx(reqCtx RequestContext, credential common.Credential) (objInfo *meta.Object, err error)
	PutObject(reqCtx RequestContext, credential common.Credential, size int64, data io.ReadCloser,
		metadata map[string]string, acl datatype.Acl,
		sse datatype.SseRequest, storageClass meta.StorageClass) (result datatype.PutObjectResult, err error)
	AppendObject(bucket, object string, credential common.Credential, offset uint64, size int64, data io.ReadCloser,
		metadata map[string]string, acl datatype.Acl,
		sse datatype.SseRequest, storageClass meta.StorageClass, objInfo *meta.Object) (result datatype.AppendObjectResult, err error)

	CopyObject(reqCtx RequestContext, targetObject *meta.Object, sourceObject *meta.Object, source io.Reader, credential common.Credential,
		sseRequest datatype.SseRequest, isMetadataOnly bool) (result datatype.PutObjectResult, err error)
	RenameObject(reqCtx RequestContext, targetObject *meta.Object, sourceObject string, credential common.Credential) (result datatype.RenameObjectResult, err error)
	PutObjectMeta(bucket *meta.Bucket, targetObject *meta.Object, credential common.Credential) (err error)
	SetObjectAcl(reqCtx RequestContext, policy datatype.AccessControlPolicy,
		acl datatype.Acl, credential common.Credential) error
	GetObjectAcl(reqCtx RequestContext, credential common.Credential) (
		policy datatype.AccessControlPolicyResponse, err error)
	DeleteObject(reqCtx RequestContext, credential common.Credential) (datatype.DeleteObjectResult,
		error)

	// Multipart operations.
	ListMultipartUploads(reqCtx RequestContext, credential common.Credential,
		request datatype.ListUploadsRequest) (result datatype.ListMultipartUploadsResponse, err error)
	NewMultipartUpload(reqCtx RequestContext, credential common.Credential,
		metadata map[string]string, acl datatype.Acl,
		sse datatype.SseRequest, storageClass meta.StorageClass) (uploadID string, err error)
	PutObjectPart(reqCtx RequestContext, credential common.Credential, uploadID string, partID int,
		size int64, data io.ReadCloser, md5Hex string,
		sse datatype.SseRequest) (result datatype.PutObjectPartResult, err error)
	CopyObjectPart(bucketName, objectName, uploadId string, partId int, size int64, data io.Reader,
		credential common.Credential, sse datatype.SseRequest) (result datatype.PutObjectResult,
		err error)
	ListObjectParts(credential common.Credential, bucket, object string,
		request datatype.ListPartsRequest) (result datatype.ListPartsResponse, err error)
	AbortMultipartUpload(reqCtx RequestContext, credential common.Credential, uploadID string) error
	CompleteMultipartUpload(reqCtx RequestContext, credential common.Credential, uploadID string,
		uploadedParts []meta.CompletePart) (result datatype.CompleteMultipartResult, err error)

	// Freezer operations.
	GetFreezer(bucketName string, objectName string, version string) (freezer *meta.Freezer, err error)
	GetFreezerStatus(bucketName string, objectName string, version string) (freezer *meta.Freezer, err error)
	CreateFreezer(freezer *meta.Freezer) (err error)
	UpdateFreezerDate(freezer *meta.Freezer, date int, isIncrement bool) (err error)
}
