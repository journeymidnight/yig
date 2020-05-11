package helper

import "testing"

func TestCaseInsensitiveEqual(t *testing.T) {
	As := []string{
		"X-Amz-Hehe",
		"AbcdefgHijklmn",
		"x-amz-content-sha256",
	}
	Bs := []string{
		"x-amz-hehe",
		"abcdefghijklmn",
		"X-Amz-Content-Sha256",
	}
	for i := 0; i < len(As); i++ {
		if CaseInsensitiveEqual(As[i], Bs[i]) == false {
			t.Error("False:", As[i], Bs[i])
		}
		if CaseInsensitiveEqual(Bs[i], As[i]) == false {
			t.Error("False:", Bs[i], As[i])
		}
	}
}
