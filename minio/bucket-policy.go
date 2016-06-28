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
)

// readBucketPolicy - read bucket policy.
func readBucketPolicy(bucket string) ([]byte, error) {
	// Verify bucket is valid.
	if !IsValidBucketName(bucket) {
		return nil, BucketNameInvalid{Bucket: bucket}
	}

	// TODO re-implement
	return
}

// removeBucketPolicy - remove bucket policy.
func removeBucketPolicy(bucket string) error {
	// Verify bucket is valid.
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	// TODO re-implement
	return
}

// writeBucketPolicy - save bucket policy.
func writeBucketPolicy(bucket string, accessPolicyBytes []byte) error {
	// Verify if bucket path legal
	if !IsValidBucketName(bucket) {
		return BucketNameInvalid{Bucket: bucket}
	}

	// TODO re-implement
	return
}
