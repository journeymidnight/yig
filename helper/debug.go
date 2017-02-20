package helper

func Debug(format string, args ...interface{}) {
	if CONFIG.DebugMode == true {
		Logger.Printf(0, format, args...)
	}
}

func Debugln(args ...interface{}) {
	if CONFIG.DebugMode == true {
		Logger.Println(0, args...)
	}
}
