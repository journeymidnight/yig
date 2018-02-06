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
	meta "github.com/journeymidnight/yig/meta/types"
	"net/http"
)

const (
	MIN_PART_SIZE = 128 << 10 // 128KB
)

// writeErrorResponsePartTooSmall - function is used specifically to
// construct a proper error response during CompleteMultipartUpload
// when one of the parts is < MIN_PART_SIZE
// The requirement comes due to the fact that generic ErrorResponse
// XML doesn't carry the additional fields required to send this
// error. So we construct a new type which lies well within the scope
// of this function.
func writePartSmallErrorResponse(w http.ResponseWriter, r *http.Request, err meta.PartTooSmall) {
	// Represents additional fields necessary for ErrPartTooSmall S3 error.
	type completeMultipartAPIError struct {
		// Proposed size represents uploaded size of the part.
		ProposedSize int64
		// Minimum size allowed epresents the minimum size allowed per
		// part. Defaults to 5MB.
		MinSizeAllowed int64
		// Part number of the part which is incorrect.
		PartNumber int
		// ETag of the part which is incorrect.
		PartETag string
		// Other default XML error responses.
		ApiErrorResponse
	}
	// Generate complete multipart error response.
	cmpErrResp := completeMultipartAPIError{
		ProposedSize:   err.PartSize,
		MinSizeAllowed: MIN_PART_SIZE,
		PartNumber:     err.PartNumber,
		PartETag:       err.PartETag,
		ApiErrorResponse: ApiErrorResponse{
			AwsErrorCode: "EntityTooSmall",
			Message:      "Your proposed upload is smaller than the minimum allowed object size.",
		},
	}
	encodedErrorResponse := EncodeResponse(cmpErrResp)
	// Write error body
	w.WriteHeader(http.StatusBadRequest)
	w.Write(encodedErrorResponse)
	w.(http.Flusher).Flush()
}
