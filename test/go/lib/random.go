package lib

import (
	"math/rand"
	"time"
)

const (
	chars    = "0123456789_abcdefghijkl-mnopqrstuvwxyz" //ABCDEFGHIJKLMNOPQRSTUVWXYZ
	charsLen = len(chars)
	mask     = 1<<6 - 1
)

var rng = rand.NewSource(time.Now().UnixNano())

// RandBytes return the random byte sequence.
func RandBytes(ln int) []byte {
	/* chars 38 characters.
	 * rng.Int64() we can use 10 time since it produces 64-bit random digits and we use 6bit(2^6=64) each time.
	 */
	buf := make([]byte, ln)
	for idx, cache, remain := ln-1, rng.Int63(), 10; idx >= 0; {
		if remain == 0 {
			cache, remain = rng.Int63(), 10
		}
		buf[idx] = chars[int(cache&mask)%charsLen]
		cache >>= 6
		remain--
		idx--
	}
	return buf
}
