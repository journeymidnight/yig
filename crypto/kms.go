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

import (
	"fmt"
	"io"
	"sort"
)

// Context is a list of key-value pairs cryptographically
// associated with a certain object.
type Context map[string]string

// WriteTo writes the context in a canonical from to w.
// It returns the number of bytes and the first error
// encounter during writing to w, if any.
//
// WriteTo sorts the context keys and writes the sorted
// key-value pairs as canonical JSON object to w.
func (c Context) WriteTo(w io.Writer) (n int64, err error) {
	sortedKeys := make(sort.StringSlice, 0, len(c))
	for k := range c {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Sort(sortedKeys)

	nn, err := io.WriteString(w, "{")
	if err != nil {
		return n + int64(nn), err
	}
	n += int64(nn)
	for i, k := range sortedKeys {
		s := fmt.Sprintf("\"%s\":\"%s\",", k, c[k])
		if i == len(sortedKeys)-1 {
			s = s[:len(s)-1] // remove last ','
		}

		nn, err = io.WriteString(w, s)
		if err != nil {
			return n + int64(nn), err
		}
		n += int64(nn)
	}
	nn, err = io.WriteString(w, "}")
	return n + int64(nn), err
}

// KMS represents an active and authenticted connection
// to a Key-Management-Service. It supports generating
// data key generation and unsealing of KMS-generated
// data keys.
type KMS interface {
	// GenerateKey generates a new random data key using
	// the master key referenced by the keyID. It returns
	// the plaintext key and the sealed plaintext key
	// on success.
	//
	// The context is cryptographically bound to the
	// generated key. The same context must be provided
	// again to unseal the generated key.
	GenerateKey(keyID string, context Context) (key [32]byte, sealedKey []byte, err error)

	// UnsealKey unseals the sealedKey using the master key
	// referenced by the keyID. The provided context must
	// match the context used to generate the sealed key.
	UnsealKey(keyID string, sealedKey []byte, context Context) (key [32]byte, err error)

	GetKeyID() string
}
