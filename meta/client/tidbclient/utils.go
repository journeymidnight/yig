package tidbclient

import "strings"

func getPlaceHolders(n, tupleCount int) string {
	if n == 0 || tupleCount == 0 {
		return ""
	}
	var b strings.Builder
	if tupleCount == 1 {
		for i := 0; i < n; i++ {
			if i != n-1 {
				b.WriteString("?,")
			} else {
				b.WriteString("?")
			}
		}
	} else {
		for i := 0; i < n; i++ {
			b.WriteString("(")
			for j := 0; j < tupleCount; j++ {
				if j != tupleCount-1 {
					b.WriteString("?,")
				} else {
					b.WriteString("?")
				}
			}
			if i != n-1 {
				b.WriteString("),")
			} else {
				b.WriteString(")")
			}
		}
	}
	return b.String()
}
