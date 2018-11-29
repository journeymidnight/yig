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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"io"

	"github.com/journeymidnight/yig/helper"
)

// ObjectKey is a 256 bit secret key used to encrypt the object.
// It must never be stored in plaintext.
type ObjectKey [32]byte

// GenerateKey generates a unique ObjectKey from a 256 bit external key
// and a source of randomness. If random is nil the default PRNG of the
// system (crypto/rand) is used.
func GenerateKey(extKey [32]byte, random io.Reader) (key ObjectKey) {
	if random == nil {
		random = rand.Reader
	}
	var nonce [32]byte
	if _, err := io.ReadFull(random, nonce[:]); err != nil {
		helper.Logger.Println(5, errOutOfEntropy)
		return key
	}
	sha := sha256.New()
	sha.Write(extKey[:])
	sha.Write(nonce[:])
	sha.Sum(key[:0])
	return key
}

// SealedKey represents a sealed object key. It can be stored
// at an untrusted location.
type SealedKey struct {
	Key       [64]byte // The encrypted and authenticted object-key.
	IV        [32]byte // The random IV used to encrypt the object-key.
	Algorithm string   // The sealing algorithm used to encrypt the object key.
}

// DerivePartKey derives an unique 256 bit key from an ObjectKey and the part index.
func (key ObjectKey) DerivePartKey(id uint32) (partKey [32]byte) {
	var bin [4]byte
	binary.LittleEndian.PutUint32(bin[:], id)

	mac := hmac.New(sha256.New, key[:])
	mac.Write(bin[:])
	mac.Sum(partKey[:0])
	return partKey
}
