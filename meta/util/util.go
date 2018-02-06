package util

import (
	"encoding/hex"
	"github.com/xxtea/xxtea-go/xxtea"
)

var XXTEA_KEY = []byte("hehehehe")

func Decrypt(value string) (string, error) {
	bytes, err := hex.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(xxtea.Decrypt(bytes, XXTEA_KEY)), nil
}

func Encrypt(value string) string {
	return hex.EncodeToString(xxtea.Encrypt([]byte(value), XXTEA_KEY))
}
