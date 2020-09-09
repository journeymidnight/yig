package tikvclient

import (
	"bytes"
	"gopkg.in/bufio.v1"
)

func GenKey(args ...string) []byte {
	buf := bufio.NewBuffer([]byte{})
	for _, arg := range args {
		buf.WriteString(arg)
		buf.WriteString(TableSeparator)
	}
	key := buf.Bytes()

	return key[:len(key)-1]
}

// [Start, End) as expected by TiKV
type Range struct {
	Empty bool
	Start []byte
	End   []byte
}

func RangeIntersection(a, b Range) Range {
	if a.Empty || b.Empty {
		return Range{
			Empty: true,
		}
	}
	var result Range
	if bytes.Compare(a.Start, b.Start) < 0 {
		result.Start = b.Start
	} else {
		result.Start = a.Start
	}
	if bytes.Compare(a.End, b.End) < 0 {
		result.End = a.End
	} else {
		result.End = b.End
	}
	if bytes.Compare(result.Start, result.End) >= 0 {
		result.Empty = true
	}
	return result
}
