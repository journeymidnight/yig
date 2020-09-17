package helper

import (
	"crypto/md5"
	"encoding"
	"encoding/hex"
	"fmt"
	"hash"
	"strings"
)

// For appendable upload, we need to calculate the whole file md5 incrementally,
// so we hack "etag" field in db to store md5 calculator internal state.
// Format if state is stored:
// <md5 hex>,<internal state hex>
func Md5WriterFromEtag(etag string) (hash.Hash, error) {
	parts := strings.Split(etag, ",")
	if len(parts) != 2 {
		return md5.New(), nil
	}
	internalState, err := hex.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}
	h := md5.New()
	err = h.(encoding.BinaryUnmarshaler).UnmarshalBinary(internalState)
	return h, err
}

func Md5FromEtag(etag string) string {
	parts := strings.Split(etag, ",")
	if len(parts) != 2 {
		return etag
	}
	return parts[0]
}

func EtagWithInternalState(md5 string, md5Writer hash.Hash) string {
	state, _ := md5Writer.(encoding.BinaryMarshaler).MarshalBinary()
	return fmt.Sprintf("%s,%s", md5, hex.EncodeToString(state))
}
