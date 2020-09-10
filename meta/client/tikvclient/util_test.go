package tikvclient

import (
	"reflect"
	"testing"
)

func TestRangeIntersection(t *testing.T) {
	a1 := Range{Start: []byte("ccc"), End: []byte("zzz")}
	b1 := Range{Start: []byte("aaa"), End: []byte("hhh")}
	expected1 := Range{Start: []byte("ccc"), End: []byte("hhh")}
	out1 := RangeIntersection(a1, b1)
	if !reflect.DeepEqual(expected1, out1) {
		t.Error("case 1:", out1, expected1)
	}

	a2 := Range{Start: []byte("aaa"), End: []byte("zzz")}
	b2 := Range{Start: []byte("ccc"), End: []byte("hhh")}
	expected2 := Range{Start: []byte("ccc"), End: []byte("hhh")}
	out2 := RangeIntersection(a2, b2)
	if !reflect.DeepEqual(out2, expected2) {
		t.Error("case 2:", out2, expected2)
	}

	a3 := Range{Start: []byte("aaa"), End: []byte("bbb")}
	b3 := Range{Start: []byte("hhh"), End: []byte("zzz")}
	out3 := RangeIntersection(a3, b3)
	if !out3.Empty {
		t.Error("case 3:", out3)
	}
}
