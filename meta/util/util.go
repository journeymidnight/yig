package util

import (
	"encoding/hex"

	"github.com/xxtea/xxtea-go/xxtea"
)

var XXTEA_KEY = []byte("hehehehe")

func Decrypt(value string) ([]byte, error) {
	bytes, err := hex.DecodeString(value)
	if err != nil {
		return nil, err
	}
	return xxtea.Decrypt(bytes, XXTEA_KEY), nil
}

func DecryptToString(value string) (string, error) {
	bytes, err := hex.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(xxtea.Decrypt(bytes, XXTEA_KEY)), nil
}

func Encrypt(value []byte) string {
	return hex.EncodeToString(xxtea.Encrypt(value, XXTEA_KEY))
}
