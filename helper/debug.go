// +build debug

package helper

import "fmt"

func Debug(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

func Debugln(args ...interface{}) {
	fmt.Println(args...)
}
