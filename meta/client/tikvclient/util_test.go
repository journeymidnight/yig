package tikvclient

import (
	"testing"
)

func Test_GenKey(t *testing.T) {
	key := GenKey("a", "b", "c")
	key2 := []byte("a\\b\\c")
	if string(key) != string(key2) {
		t.Fatal("err")
	}
}
