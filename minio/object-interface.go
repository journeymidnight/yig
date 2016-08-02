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

package minio

import (
	"git.letv.cn/yig/yig/iam"
	. "git.letv.cn/yig/yig/minio/datatype"
	"io"
)

// ObjectLayer implements primitives for object API layer.
type ObjectLayer interface {
	// Bucket operations.
	MakeBucket(bucket string, credential iam.Credential) error
	GetBucketInfo(bucket string, credential iam.Credential) (bucketInfo BucketInfo, err error)
	ListBuckets(credential iam.Credential) (buckets []BucketInfo, err error)
	DeleteBucket(bucket string, credential iam.Credential) error
	ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result ListObjectsInfo, err error)

	// Object operations.
	GetObject(object ObjectInfo, startOffset int64, length int64, writer io.Writer) (err error)
	GetObjectInfo(bucket, object string) (objInfo ObjectInfo, err error)
	PutObject(bucket, object string, size int64, data io.Reader, metadata map[string]string) (md5 string, err error)
	DeleteObject(bucket, object string) error

	// Multipart operations.
	ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error)
	NewMultipartUpload(bucket, object string, metadata map[string]string) (uploadID string, err error)
	PutObjectPart(bucket, object, uploadID string, partID int, size int64, data io.Reader, md5Hex string) (md5 string, err error)
	ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result ListPartsInfo, err error)
	AbortMultipartUpload(bucket, object, uploadID string) error
	CompleteMultipartUpload(bucket, object, uploadID string, uploadedParts []CompletePart) (md5 string, err error)
}
