// +build !debug

package helper

// functions in this file do nothing as they are release version

func Debug(format string, args ...interface{}) {
}

func Debugln(args ...interface{}) {
}
