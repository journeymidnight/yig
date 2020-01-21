package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta"
	bus "github.com/journeymidnight/yig/mq"
)

type ResponseRecorder struct {
	http.ResponseWriter
	status        int
	size          int64
	operationName string
	serverCost    time.Duration
	requestTime   time.Duration
	errorCode     string

	storageClass       string
	targetStorageClass string
	bucketLogging      bool
	cdn_request        bool
}

func NewResponseRecorder(w http.ResponseWriter) *ResponseRecorder {
	return &ResponseRecorder{
		ResponseWriter: w,
		status:         http.StatusOK,
	}
}

func (r *ResponseRecorder) Flush() {
	return
}

type AccessLogHandler struct {
	handler          http.Handler
	responseRecorder *ResponseRecorder
	format           string
}

func (a AccessLogHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.responseRecorder = NewResponseRecorder(w)

	startTime := time.Now()
	a.handler.ServeHTTP(a.responseRecorder, r)
	finishTime := time.Now()
	a.responseRecorder.requestTime = finishTime.Sub(startTime)

	newReplacer := NewReplacer(r, a.responseRecorder, "-")
	response := newReplacer.Replace(a.format)

	helper.AccessLogger.Println(response)
	// send the entries in access logger to message queue.
	elems := newReplacer.GetReplacedValues()
	a.notify(elems)
}

func (a AccessLogHandler) notify(elems map[string]string) {
	if len(elems) == 0 {
		return
	}
	val, err := helper.MsgPackMarshal(elems)
	if err != nil {
		helper.Logger.Error("Failed to pack", elems, "err:", err)
		return
	}

	err = bus.MsgSender.AsyncSend(val)
	if err != nil {
		helper.Logger.Error(
			fmt.Sprintf("Failed to send message [%v] to message queue, err: %v",
				elems, err))
		return
	}
	helper.Logger.Info(fmt.Sprintf("Succeed to send message [%v] to message queue.",
		elems))
}

func NewAccessLogHandler(handler http.Handler, _ *meta.Meta) http.Handler {
	format := helper.CONFIG.AccessLogFormat
	format = strings.Replace(format, "{combined}", CombinedLogFormat, -1)
	format = strings.Replace(format, "{billing}", BillingLogFormat, -1)
	return AccessLogHandler{
		handler: handler,
		format:  format,
	}
}
