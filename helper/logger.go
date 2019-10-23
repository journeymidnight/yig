package helper

import (
	"github.com/journeymidnight/yig/log"
)

// Global singleton loggers
var Logger log.Logger
var AccessLogger log.Logger
var TracerLogger log.Factory

func PanicOnError(err error, message string) {
	if err != nil {
		panic(message + " " + err.Error())
	}
}
