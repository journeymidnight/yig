package sts

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"github.com/golang/protobuf/proto"
	"io"
)

/*
Packed binary V1 format:
byte 0: version, 1, 1 byte
byte 1-12: nonce(initialization vector), 12 bytes
byte 13-end: FederationToken, first encoded in protobuf,
			then encrypted use AES-256-GCM.

Returned string is base64 encoded.
*/

func (m *FederationToken) PackV1(key []byte) (string, error) {
	tokenBytes, err := proto.Marshal(m)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, 12)
	_, err = io.ReadFull(rand.Reader, nonce)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	cipherText := aesgcm.Seal(nil, nonce, tokenBytes, nil)

	buf := new(bytes.Buffer)
	buf.Write([]byte{byte(1)})
	buf.Write(nonce)
	buf.Write(cipherText)

	base64String := base64.StdEncoding.EncodeToString(buf.Bytes())
	return base64String, nil
}

func UnpackV1(key []byte, data string) (FederationToken, error) {
	binary, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return FederationToken{}, err
	}
	if binary[0] != byte(1) {
		return FederationToken{}, errors.New("unsupported pack version")
	}
	nonce := binary[1:13]
	cipherText := binary[13:]

	block, err := aes.NewCipher(key)
	if err != nil {
		return FederationToken{}, err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return FederationToken{}, err
	}
	plainText, err := aesgcm.Open(nil, nonce, cipherText, nil)
	if err != nil {
		return FederationToken{}, err
	}
	var token FederationToken
	err = proto.Unmarshal(plainText, &token)
	return token, err
}
