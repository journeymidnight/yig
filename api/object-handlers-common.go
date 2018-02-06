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
	"net/http"
	"strings"
	"time"

	. "github.com/journeymidnight/yig/error"
	meta "github.com/journeymidnight/yig/meta/types"
)

// Validates the preconditions for CopyObject, returns nil if validates
// Preconditions supported are:
//  x-amz-copy-source-if-modified-since
//  x-amz-copy-source-if-unmodified-since
//  x-amz-copy-source-if-match
//  x-amz-copy-source-if-none-match
func checkObjectPreconditions(w http.ResponseWriter, r *http.Request, object *meta.Object) error {
	// x-amz-copy-source-if-modified-since: Return the object only if it has been modified
	// since the specified time
	ifModifiedSinceHeader := r.Header.Get("x-amz-copy-source-if-modified-since")
	if ifModifiedSinceHeader != "" {
		givenTime, err := time.Parse(http.TimeFormat, ifModifiedSinceHeader)
		if err != nil {
			return ErrInvalidPrecondition
		}
		if object.LastModifiedTime.Before(givenTime) {
			// If the object is not modified since the specified time.
			return ErrPreconditionFailed
		}
	}

	// x-amz-copy-source-if-unmodified-since : Return the object only if it has not been
	// modified since the specified time
	ifUnmodifiedSinceHeader := r.Header.Get("x-amz-copy-source-if-unmodified-since")
	if ifUnmodifiedSinceHeader != "" {
		givenTime, err := time.Parse(http.TimeFormat, ifUnmodifiedSinceHeader)
		if err != nil {
			return ErrInvalidPrecondition
		}
		if object.LastModifiedTime.After(givenTime) {
			// If the object is modified since the specified time.
			return ErrPreconditionFailed
		}
	}

	// x-amz-copy-source-if-match : Return the object only if its entity tag (ETag) is the
	// same as the one specified
	ifMatchETagHeader := r.Header.Get("x-amz-copy-source-if-match")
	if ifMatchETagHeader != "" {
		if !isETagEqual(object.Etag, ifMatchETagHeader) {
			// If the object ETag does not match with the specified ETag.
			return ErrPreconditionFailed
		}
	}

	// If-None-Match : Return the object only if its entity tag (ETag) is different from the
	// one specified
	ifNoneMatchETagHeader := r.Header.Get("x-amz-copy-source-if-none-match")
	if ifNoneMatchETagHeader != "" {
		if isETagEqual(object.Etag, ifNoneMatchETagHeader) {
			// If the object ETag matches with the specified ETag.
			return ErrPreconditionFailed
		}
	}

	if ifNoneMatchETagHeader != "" && ifUnmodifiedSinceHeader != "" {
		return ErrInvalidPrecondition
	}
	if ifMatchETagHeader != "" && ifModifiedSinceHeader != "" {
		return ErrInvalidPrecondition
	}

	return nil
}

// Validates the preconditions for GetObject/HeadObject. Returns nil if validates
// Preconditions supported are:
//  If-Modified-Since
//  If-Unmodified-Since
//  If-Match
//  If-None-Match
func checkPreconditions(header http.Header, object *meta.Object) error {
	// If-Modified-Since : Return the object only if it has been modified since the specified time,
	// otherwise return a 304 (not modified).
	ifModifiedSinceHeader := header.Get("If-Modified-Since")
	if ifModifiedSinceHeader != "" {
		givenTime, err := time.Parse(http.TimeFormat, ifModifiedSinceHeader)
		if err != nil {
			return ErrInvalidPrecondition
		}
		if object.LastModifiedTime.Before(givenTime) {
			// If the object is not modified since the specified time.
			return ContentNotModified
		}
	}

	// If-Unmodified-Since : Return the object only if it has not been modified since the specified
	// time, otherwise return a 412 (precondition failed).
	ifUnmodifiedSinceHeader := header.Get("If-Unmodified-Since")
	if ifUnmodifiedSinceHeader != "" {
		givenTime, err := time.Parse(http.TimeFormat, ifUnmodifiedSinceHeader)
		if err != nil {
			return ErrInvalidPrecondition
		}
		if object.LastModifiedTime.After(givenTime) {
			return ErrPreconditionFailed
		}
	}

	// If-Match : Return the object only if its entity tag (ETag) is the same as the one specified;
	// otherwise return a 412 (precondition failed).
	ifMatchETagHeader := header.Get("If-Match")
	if ifMatchETagHeader != "" {
		if !isETagEqual(object.Etag, ifMatchETagHeader) {
			// If the object ETag does not match with the specified ETag.
			return ErrPreconditionFailed
		}
	}

	// If-None-Match : Return the object only if its entity tag (ETag) is different from the
	// one specified otherwise, return a 304 (not modified).
	ifNoneMatchETagHeader := header.Get("If-None-Match")
	if ifNoneMatchETagHeader != "" {
		if isETagEqual(object.Etag, ifNoneMatchETagHeader) {
			// If the object ETag matches with the specified ETag.
			return ContentNotModified
		}
	}
	return nil
}

// canonicalizeETag returns ETag with leading and trailing double-quotes removed,
// if any present
func canonicalizeETag(etag string) string {
	canonicalETag := strings.TrimPrefix(etag, "\"")
	return strings.TrimSuffix(canonicalETag, "\"")
}

// isETagEqual return true if the canonical representations of two ETag strings
// are equal, false otherwise
func isETagEqual(left, right string) bool {
	return canonicalizeETag(left) == canonicalizeETag(right)
}
