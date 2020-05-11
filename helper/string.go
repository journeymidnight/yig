package helper

import "strings"

func HasBucketInDomain(host string, domains []string) (ok bool, bucket string) {
	for _, d := range domains {
		suffix := "." + d
		if strings.HasSuffix(host, suffix) {
			return true, strings.TrimSuffix(host, suffix)
		}
	}
	return false, ""
}

func StringInSlice(s string, ss []string) bool {
	for _, x := range ss {
		if s == x {
			return true
		}
	}
	return false
}

// compare 2 strings case-insensitively, only handle ASCII
func CaseInsensitiveEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		aChar := a[i]
		bChar := b[i]
		if aChar == bChar {
			continue
		}
		if 'A' <= aChar && aChar <= 'Z' && aChar+'a'-'A' == bChar {
			continue
		}
		if 'A' <= bChar && bChar <= 'Z' && bChar+'a'-'A' == aChar {
			continue
		}
		return false
	}
	return true
}

func StringInSliceCustomCompare(s string, ss []string,
	compare func(string, string) bool) bool {

	for _, x := range ss {
		if compare(x, s) {
			return true
		}
	}
	return false
}
