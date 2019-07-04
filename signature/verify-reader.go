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

package signature

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"net/http"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/iam/common"
)

// SignVerifyReader represents an io.Reader compatible interface which
// transparently calculates SHA256 for v4 signed authentication.
// Caller should call `SignVerifyReader.Verify()` to validate the signature header.
type SignVerifyReader struct {
	Request      *http.Request
	Reader       io.Reader
	Sha256Writer hash.Hash
}

// Initializes a new signature verify reader.
func newSignVerify(req *http.Request) *SignVerifyReader {
	// do not need to calculate SHA256 when header is unsigned
	if req.Header.Get("x-amz-content-sha256") == UnsignedPayload {
		return &SignVerifyReader{
			Request:      req,
			Reader:       req.Body,
			Sha256Writer: nil,
		}
	}

	sha256Writer := sha256.New()
	reader := io.TeeReader(req.Body, sha256Writer)
	return &SignVerifyReader{
		Request:      req,
		Reader:       reader,
		Sha256Writer: sha256Writer,
	}
}

// Verify - verifies signature and returns error upon signature mismatch.
func (v *SignVerifyReader) Verify() (common.Credential, error) {
	var payloadSha256Hex string
	if v.Sha256Writer != nil {
		payloadSha256Hex = hex.EncodeToString(v.Sha256Writer.Sum(nil))
	} else {
		payloadSha256Hex = UnsignedPayload
	}
	return DoesSignatureMatchV4(payloadSha256Hex, v.Request, true)
}

func (v *SignVerifyReader) Read(b []byte) (int, error) {
	return v.Reader.Read(b)
}

func VerifyUpload(r *http.Request) (credential common.Credential, dataReader io.Reader, err error) {
	dataReader = r.Body
	switch GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		err = ErrAccessDenied
		return
	case AuthTypeAnonymous:
		break
	case AuthTypeSignedV2:
		credential, err = DoesSignatureMatchV2(r)
	case AuthTypeSignedV4:
		credential, err = getCredentialUnverified(r)
		dataReader = newSignVerify(r)
	case AuthTypePresignedV2:
		credential, err = DoesPresignedSignatureMatchV2(r)
	case AuthTypePresignedV4:
		credential, err = DoesPresignedSignatureMatchV4(r, true)
	case AuthTypeStreamingSigned:
		chunkReader, err := newSignV4ChunkedReader(r)
		if err != nil {
			return credential, nil, err
		}
		return chunkReader.(*s3ChunkedReader).cred, chunkReader, err
	}
	return
}
