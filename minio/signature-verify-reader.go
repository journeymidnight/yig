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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"

	. "git.letv.cn/yig/yig/minio/datatype"
	"git.letv.cn/yig/yig/signature"
	"git.letv.cn/yig/yig/iam"
)

// SignVerifyReader represents an io.Reader compatible interface which
// transparently calculates sha256, caller should call `Verify()` to
// validate the signature header.
type SignVerifyReader struct {
	Request    *http.Request // HTTP request to be validated and read.
	HashWriter hash.Hash     // sha256 hash writer.
}

// Initializes a new signature verify reader.
func newSignVerify(req *http.Request) *SignVerifyReader {
	return &SignVerifyReader{
		Request:    req,          // Save the request.
		HashWriter: sha256.New(), // Inititalize sha256.
	}
}

// isSignVerify - is given reader a `signVerifyReader`.
func isSignVerify(reader io.Reader) bool {
	_, ok := reader.(*SignVerifyReader)
	return ok
}

// Verify - verifies signature and returns error upon signature mismatch.
func (v *SignVerifyReader) Verify() (credential iam.Credential, err error) {
	validateRegion := true // Defaults to validating region.
	shaPayloadHex := hex.EncodeToString(v.HashWriter.Sum(nil))
	if skipContentSha256Cksum(v.Request) {
		// Sets 'UNSIGNED-PAYLOAD' if client requested to not calculated sha256.
		shaPayloadHex = unsignedPayload
	}
	// Signature verification block.
	var s3Error APIErrorCode
	if isSignature, version := isRequestSignature(v.Request); isSignature {
		if version == authTypeSignedV2 {
			credential, s3Error = signature.DoesSignatureMatchV2(v.Request)
		} else { // v4
			credential, s3Error = doesSignatureMatch(shaPayloadHex, v.Request, validateRegion)
		}
	} else if isPresigned, version := isRequestPresigned(v.Request); isPresigned {
		if version == authTypePresignedV2 {
			credential, s3Error = signature.DoesPresignedSignatureMatch(v.Request)
		} else { // v4
			credential, s3Error = doesPresignedSignatureMatch(v.Request, validateRegion)
		}
	} else {
		// Couldn't figure out the request type, set the error as AccessDenied.
		s3Error = ErrAccessDenied
	}
	// Validate if we have received signature mismatch or sha256 mismatch.
	if s3Error != ErrNone {
		switch s3Error {
		case ErrContentSHA256Mismatch:
			err = ErrorContentSHA256Mismatch
		case ErrSignatureDoesNotMatch:
			err = ErrSignatureMismatch
		default:
			err = fmt.Errorf("%v", GetAPIError(s3Error))
		}
		return
	}
	return
}

// Reads from request body and writes to hash writer. All reads performed
// through it are matched with corresponding writes to hash writer. There is
// no internal buffering the write must complete before the read completes.
// Any error encountered while writing is reported as a read error. As a
// special case `Read()` skips writing to hash writer if the client requested
// for the payload to be skipped.
func (v *SignVerifyReader) Read(b []byte) (n int, err error) {
	n, err = v.Request.Body.Read(b)
	if n > 0 {
		// Skip calculating the hash.
		if skipContentSha256Cksum(v.Request) {
			return
		}
		// Stagger all reads to its corresponding writes to hash writer.
		if n, err = v.HashWriter.Write(b[:n]); err != nil {
			return n, err
		}
	}
	return
}
