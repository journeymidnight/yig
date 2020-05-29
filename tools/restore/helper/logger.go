package helper

import (
	"github.com/journeymidnight/yig-restore/log"
)

var Logger log.Logger

func PanicOnError(err error, message string) {
	if err != nil {
		panic(message + " " + err.Error())
	}
}
