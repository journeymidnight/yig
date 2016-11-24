package helper

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
