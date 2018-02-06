package types

import "testing"

func TestLowerBound(t *testing.T) {
	s := &SimpleIndex{
		Index: []int64{-10, -4, 0, 1, 10, 11, 40, 50, 51, 90},
	}

	var testcase = [...]struct {
		value int64
		pos   int
	}{
		//LowerBound(-10) = 0
		{-10, 0},
		{-3, 1},
		{0, 2},
		{56, 8},
		{20, 5},
		{90, 9},
		{89, 8},
		{49, 6},
		{100, 9},
		{-100, -1},
	}

	for _, v := range testcase {
		ret := s.SearchLowerBound(v.value)
		if ret != v.pos {
			t.Errorf("Search LowerBound for %d failed, expected %d, got %d\n", v.value, v.pos, ret)
		}
	}

}

func TestUpperBound(t *testing.T) {
	s := &SimpleIndex{
		Index: []int64{-10, -4, 0, 1, 10, 11, 40, 50, 51, 90},
	}

	var testcase = [...]struct {
		value int64
		pos   int
	}{
		//UpperBound(-10) = 1
		{-10, 1},
		{-3, 2},
		{0, 3},
		{56, 9},
		{20, 6},
		{90, -1},
		{89, 9},
		{49, 7},
		{100, -1},
		{39, 6},
	}

	for _, v := range testcase {
		ret := s.SearchUpperBound(v.value)
		if ret != v.pos {
			t.Errorf("Search UpperBound for %d failed, expected %d, got %d\n", v.value, v.pos, ret)
		}
	}
}
