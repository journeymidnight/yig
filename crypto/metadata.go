// Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crypto

const (
	SSECAlgorithm = "X-Amz-Server-Side-Encryption-Customer-Algorithm"
	SSECKey       = "X-Amz-Server-Side-Encryption-Customer-Key"
	SSECKeyMD5    = "X-Amz-Server-Side-Encryption-Customer-Key-Md5"
	SSECopyKeyMD5 = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5"
	SSECopyKey    = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key"
)

// RemoveSensitiveEntries removes confidential encryption
// information - e.g. the SSE-C key - from the metadata map.
// It has the same semantics as RemoveSensitiveHeaders.
func RemoveSensitiveEntries(metadata map[string]string) { // The functions is tested in TestRemoveSensitiveHeaders for compatibility reasons
	delete(metadata, SSECKey)
	delete(metadata, SSECopyKey)
}

// IsETagSealed returns true if the etag seems to be encrypted.
func IsETagSealed(etag []byte) bool { return len(etag) > 16 }
