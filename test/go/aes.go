package _go

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
)

var iv = []byte{0x31, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31, 0x38, 0x27, 0x36, 0x35, 0x33, 0x23, 0x32, 0x33}

func pKCS7Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func pKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

func AESEncrypt(data, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCEncrypter(block, iv)
	data = pKCS7Padding(data, blockSize)

	var cryptCode = make([]byte, len(data))
	blockMode.CryptBlocks(cryptCode, data)
	return cryptCode, nil
}

func AESEncryptToHexString(data, key []byte) (string, error) {
	r, err := AESEncrypt(data, key)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(r), nil
}

func AESDecrypt(cryptData, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockMode := cipher.NewCBCDecrypter(block, iv)
	origData := make([]byte, len(cryptData))
	blockMode.CryptBlocks(origData, cryptData)
	origData = pKCS7UnPadding(origData)
	return origData, nil
}

func AESDecryptHexStringToOrigin(hexStr string, key []byte) (string, error) {
	in, err := hex.DecodeString(hexStr)
	if err != nil {
		return "", err
	}
	origin, err := AESDecrypt(in, key)
	if err != nil {
		return "", err
	}
	return string(origin), nil
}
