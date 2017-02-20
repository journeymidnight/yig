package helper

import (
	"bytes"

	"github.com/ugorji/go/codec"
)

func MsgPackMarshal(v interface{}) ([]byte, error) {
	var buf = new(bytes.Buffer)
	enc := codec.NewEncoder(buf, new(codec.MsgpackHandle))
	err := enc.Encode(v)
	return buf.Bytes(), err
}
func MsgPackUnMarshal(data []byte, v interface{}) error {
	var buf = bytes.NewBuffer(data)
	dec := codec.NewDecoder(buf, new(codec.MsgpackHandle))
	return dec.Decode(v)
}
