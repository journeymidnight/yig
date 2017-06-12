package helper

import (
	"os"
)

func FileExists(path string) bool {
	st, e := os.Stat(path)
	// If file exists and is regular return true.
	if e == nil && st.Mode().IsRegular() {
		return true
	}
	return false
}
