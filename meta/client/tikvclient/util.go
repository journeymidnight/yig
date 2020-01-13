package tikvclient

import (
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
