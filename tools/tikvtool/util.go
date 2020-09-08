package main

import (
	"bytes"
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/journeymidnight/yig/meta/client/tikvclient"
)

func ParseToBytes(s string) (bs []byte, err error) {
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	ss := strings.Split(s, " ")
	for _, v := range ss {
		i, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}
		if i < 0 || i > 255 {
			return nil, errors.New("Invalid bytes")
		}
		bs = append(bs, byte(i))
	}
	return
}

func FormatKey(key []byte) string {
	b := bytes.Replace(key, []byte(tikvclient.TableSeparator), []byte("\\"), -1)
	return string(b)
}

func DecodeKey(k []byte) []byte {
	return bytes.Replace(k, []byte("\\"), []byte{31}, -1)
}

// can be printed
func EncodeKey(k []byte) []byte {
	return bytes.Replace(k, []byte{31}, []byte("\\"), -1)
}

// remove '(' and '),' or ');'
func trimLineSides(s []byte) []byte {
	if len(s) < 3 {
		return s
	}
	s = s[1:]        // remove '('
	s = s[:len(s)-2] // remove '),' or ');'
	return s
}

func trimSides(s []byte) []byte {
	if s[0] != '\'' {
		return s
	}
	if len(s) < 2 {
		return s
	}
	s = s[1:]
	s = s[:len(s)-1]
	return s
}

var (
	DmlJsonPrefix = []byte("CONVERT('")
	DmlJsonSuffix = []byte("' USING UTF8MB4)")
)

func extractConstant(ref *[]byte) (data []byte) {
	if ref == nil {
		return []byte{}
	}
	if len(*ref) == 0 {
		return []byte{}
	}
	sp := bytes.SplitN(*ref, []byte(","), 2)
	if len(sp) < 2 {
		*ref = nil
	} else {
		*ref = sp[1]
	}
	data = trimSides(sp[0])
	if bytes.Compare(data, []byte("NULL")) == 0 {
		data = nil
	}
	return data
}

func extractJson(ref *[]byte) (data []byte) {
	remain := *ref
	data = *ref
	data = data[len(DmlJsonPrefix):]
	data = data[:bytes.Index(data, DmlJsonSuffix)]
	remain = remain[len(DmlJsonPrefix)+len(data)+len(DmlJsonSuffix):]
	if len(remain) > 0 {
		remain = remain[1:] // skip ','
	}
	*ref = remain
	return
}

//FIXME: need wonder other situation?
func removeEscapedChar(s []byte) []byte {
	return bytes.Replace(s, []byte("\\\""), []byte("\""), -1)
}

// no memory copy
func toString(b []byte) string {
	h := *(*reflect.SliceHeader)(unsafe.Pointer(&b))
	return *(*string)(unsafe.Pointer(&h))
}

func tidyDml(dml []byte) []byte {
	if dml[0] != '(' {
		return nil
	}
	dml = trimLineSides(dml)
	dml = removeEscapedChar(dml)
	return dml
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
