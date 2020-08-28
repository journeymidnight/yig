package main

import (
	"bytes"
	"testing"
)

var EncodedKey = "b\\test1"
var Key = []byte{98, 31, 116, 101, 115, 116, 49}

func Test_DecodeKey(t *testing.T) {
	k := []byte(EncodedKey)
	k = DecodeKey(k)
	if bytes.Compare(Key, k) != 0 {
		t.Fatal("invalid decode key")
	}
}

func Test_EncodeKey(t *testing.T) {
	k := EncodeKey(Key)
	if string(k) != EncodedKey {
		t.Fatal("invalid encode key")
	}
}
