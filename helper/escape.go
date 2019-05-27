package helper

import (
	"bytes"
)

func EscapeColon(s string) string {
	//Byte loop is OK for utf8
	buf := new(bytes.Buffer)
	l := len(s)
	for i := 0; i < l; i++ {
		if s[i] == '%' {
			buf.WriteString("%%")
		} else if s[i] == ':' {
			buf.WriteString("%n")
		} else {
			buf.WriteByte(s[i])
		}
	}
	return buf.String()
}

func UnescapeColon(s string) string {
	buf := new(bytes.Buffer)
	l := len(s)
	for i := 0; i < l; i++ {
		if s[i] == '%' {
			i++
			if i == l {
				panic("never be here")
			}
			if s[i] == '%' {
				buf.WriteString("%")
			} else if s[i] == 'n' {
				buf.WriteString(":")
			} else {
				panic("never be here")
			}
		} else {
			buf.WriteByte(s[i])
		}
	}
	return buf.String()
}
