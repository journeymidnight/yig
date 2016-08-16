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
	"git.letv.cn/yig/yig/meta"
)

// readBucketPolicy - read bucket policy.
func readBucketPolicy(bucket string) (policy []byte, err error) {
	// Verify bucket is valid.
	if !IsValidBucketName(bucket) {
		return nil, meta.BucketNameInvalid{Bucket: bucket}
	}

	// TODO re-implement
	return
}

// removeBucketPolicy - remove bucket policy.
func removeBucketPolicy(bucket string) error {
	// Verify bucket is valid.
	if !IsValidBucketName(bucket) {
		return meta.BucketNameInvalid{Bucket: bucket}
	}

	// TODO re-implement
	return nil
}

// writeBucketPolicy - save bucket policy.
func writeBucketPolicy(bucket string, accessPolicyBytes []byte) error {
	// Verify if bucket path legal
	if !IsValidBucketName(bucket) {
		return meta.BucketNameInvalid{Bucket: bucket}
	}

	// TODO re-implement
	return nil
}
