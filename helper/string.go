package helper

import "strings"

func HasBucketInDomain(host string, prefix string, domains []string) (ok bool, bucket string)  {
	for _, d := range domains {
		if strings.HasSuffix(host, prefix+d) {
			return true, strings.TrimSuffix(host, prefix+d)
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

func CopiedBytes(source []byte) (destination []byte) {
	destination = make([]byte, len(source), len(source))
	copy(destination, source)
	return destination
}

func UnicodeIndex(str, substr string) int {
	result := strings.Index(str, substr)
	if result >= 0 {
		prefix := []byte(str)[0:result]
		rs := []rune(string(prefix))
		result = len(rs)
	}
	return result
}

func SubString(str string, begin, length int) (substr string) {
	rs := []rune(str)
	lth := len(rs)
	if begin < 0 {
		begin = 0
	}
	if begin >= lth {
		begin = lth
	}
	var end int
	if length == -1 {
		end = lth
	} else {
		end = begin + length
	}
	if end > lth {
		end = lth
	}
	return string(rs[begin:end])
}
