package log

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
)

type Level int

const (
	FatalLevel Level = 0 // Errors must be properly handled
	ErrorLevel Level = 1 // Errors should be handled, maybe not too urgent
	WarnLevel  Level = 2 // Errors could be ignored; messages might need noticed
	InfoLevel  Level = 3 // Informational messages
	DebugLevel Level = 4 // Debug messages
)

func ParseLevel(levelString string) Level {
	switch strings.ToLower(levelString) {
	case "info":
		return InfoLevel
	case "warn":
		return WarnLevel
	case "error":
		return ErrorLevel
	case "fatal":
		return FatalLevel
	case "debug":
		return DebugLevel
	default:
		return InfoLevel
	}
}

type Logger struct {
	filePath  string // the underlying log file path
	out       io.WriteCloser
	level     Level
	logger    *log.Logger
	requestID string
}

var logFlags = log.Ldate | log.Ltime | log.Lmicroseconds

func NewFileLogger(path string, logLevel Level) Logger {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + path)
	}
	l := NewLogger(f, logLevel)
	l.filePath = path
	return l
}

func NewLogger(out io.WriteCloser, logLevel Level) Logger {
	l := Logger{
		out:    out,
		level:  logLevel,
		logger: log.New(out, "", logFlags),
	}
	return l
}

func (l Logger) GetLogger() *log.Logger {
	return l.logger
}

func (l Logger) NewWithRequestID(requestID string) Logger {
	return Logger{
		out:       l.out,
		level:     l.level,
		logger:    l.logger,
		requestID: requestID,
	}
}

func getCaller(skipCallDepth int) string {
	_, fullPath, line, ok := runtime.Caller(skipCallDepth)
	if !ok {
		return ""
	}
	fileParts := strings.Split(fullPath, "/")
	file := fileParts[len(fileParts)-2] + "/" + fileParts[len(fileParts)-1]
	return fmt.Sprintf("%s:%d", file, line)
}

func (l Logger) prefixArray() []interface{} {
	array := make([]interface{}, 0, 3)
	array = append(array, getCaller(3))
	if len(l.requestID) > 0 {
		array = append(array, l.requestID)
	}
	return array
}

func (l Logger) Log(level Level, args ...interface{}) {
	prefixArray := l.prefixArray()
	switch level {
	case InfoLevel:
		if l.level < InfoLevel {
			return
		}
		prefixArray = append(prefixArray, "[INFO]")
		break
	case WarnLevel:
		if l.level < WarnLevel {
			return
		}
		prefixArray = append(prefixArray, "[WARN]")
		break
	case ErrorLevel:
		if l.level < ErrorLevel {
			return
		}
		prefixArray = append(prefixArray, "[ERROR]")
		break
	case FatalLevel:
		if l.level < FatalLevel {
			return
		}
		prefixArray = append(prefixArray, "[FATAl]")
		break
	case DebugLevel:
		if l.level < DebugLevel {
			return
		}
		prefixArray = append(prefixArray, "[DEBUG]")
	}
	args = append(prefixArray, args...)
	l.logger.Println(args...)
}

func (l Logger) Info(args ...interface{}) {
	if l.level < InfoLevel {
		return
	}
	prefixArray := l.prefixArray()
	prefixArray = append(prefixArray, "[INFO]")
	args = append(prefixArray, args...)
	l.logger.Println(args...)
}

func (l Logger) Warn(args ...interface{}) {
	if l.level < WarnLevel {
		return
	}
	prefixArray := l.prefixArray()
	prefixArray = append(prefixArray, "[WARN]")
	args = append(prefixArray, args...)
	l.logger.Println(args...)
}

func (l Logger) Error(args ...interface{}) {
	if l.level < ErrorLevel {
		return
	}
	prefixArray := l.prefixArray()
	prefixArray = append(prefixArray, "[ERROR]")
	args = append(prefixArray, args...)
	l.logger.Println(args...)
}

func (l Logger) Fatal(args ...interface{}) {
	if l.level < FatalLevel {
		return
	}
	prefixArray := l.prefixArray()
	prefixArray = append(prefixArray, "[FATAl]")
	args = append(prefixArray, args...)
	l.logger.Println(args...)
}

func (l Logger) Debug(args ...interface{}) {
	if l.level < DebugLevel {
		return
	}
	prefixArray := l.prefixArray()
	prefixArray = append(prefixArray, "[DEBUG]")
	args = append(prefixArray, args...)
	l.logger.Println(args...)
}

// Write a new line with args. Unless you really want to customize
// output format, use "Info", "Warn", "Error" instead
func (l Logger) Println(args ...interface{}) {
	_, _ = l.out.Write([]byte(fmt.Sprintln(args...)))
}

func (l Logger) Close() error {
	return l.out.Close()
}

func (l *Logger) ReopenLogFile() {
	if len(l.filePath) == 0 {
		return
	}
	newFile, err := os.OpenFile(l.filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(fmt.Sprintln("ReopenLogFile:", l.filePath, err))
	}
	newLogger := log.New(newFile, "", logFlags)
	oldFile := l.out
	l.out = newFile
	l.logger = newLogger
	_ = oldFile.Close()
}
