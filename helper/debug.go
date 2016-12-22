package helper

import "fmt"

func Debug(format string, args ...interface{}) {
	if CONFIG.DebugMode == true {
		fmt.Printf(format, args...)
	}

}

func Debugln(args ...interface{}) {
	if CONFIG.DebugMode == true {
		fmt.Println(args...)
	}
}
