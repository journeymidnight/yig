package tikvclient

import (
	"github.com/journeymidnight/yig/helper"
	"gopkg.in/bufio.v1"
)

func GenKey(args ...string) []byte {
	buf := bufio.NewBuffer([]byte{})
	for _, arg := range args {
		buf.WriteString(arg)
		buf.WriteByte(Seperator)
	}
	buf.Truncate(buf.Len() - 1)
	return buf.Bytes()
}

func EncodeValue(v interface{}) ([]byte, error) {
	return helper.MsgPackMarshal(v)
}
