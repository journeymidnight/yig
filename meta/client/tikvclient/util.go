package tikvclient

import "gopkg.in/bufio.v1"

func GenKey(delemiter bool, args ...string) []byte {
	buf := bufio.NewBuffer([]byte{})
	for _, arg := range args {
		buf.WriteString(arg)
		buf.WriteByte(TableSeparator)
	}
	key := buf.Bytes()
	if delemiter {
		return key
	} else {
		return key[:len(key)-1]
	}
}
