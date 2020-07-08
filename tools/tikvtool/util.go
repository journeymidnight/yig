package main

import (
	"bytes"
	"errors"
	"strconv"
	"strings"

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
