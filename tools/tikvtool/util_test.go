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

func Test_ExtractConstant(t *testing.T) {
	var data = []byte(`'wtf','hehe'`)
	d := extractConstant(&data)
	if toString(d) != "wtf" {
		t.Fatal("not", toString(d), "should be wtf")
	}
	if toString(data) != `'hehe'` {
		t.Fatal("not", toString(data), "should be wtf")
	}
}

func Test_ExtractJson(t *testing.T) {
	data := []byte(`'{\"CannedAcl\": \"private\"}',remain`)
	s := extractJson(&data)
	if bytes.Compare(s, []byte(`{\"CannedAcl\": \"private\"}`)) != 0 {
		t.Fatal("trimJson err data", string(s))
	}
	if bytes.Compare(data, []byte(`remain`)) != 0 {
		t.Fatal("trimJson err remain", string(data))
	}
}

func Test_ToString(t *testing.T) {
	s := toString([]byte("hehe"))
	if s != "hehe" {
		t.Fatal(s, "is not hehe")
	}
}
