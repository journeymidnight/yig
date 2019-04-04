package api

import (
	"github.com/journeymidnight/yig/helper"
	"net/http"
	"time"
)

type ResponseRecorder struct {
	http.ResponseWriter
	status      int
	size        int
	requestTime time.Duration
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

func (a AccessLogHandler) SetHandler(handler http.Handler, _ ObjectLayer) http.Handler {
	return AccessLogHandler{
		handler: handler,
		format:  CombinedLogFormat,
	}
}

func (a AccessLogHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.responseRecorder = NewResponseRecorder(w)

	startTime := time.Now()
	a.handler.ServeHTTP(a.responseRecorder, r)
	finishTime := time.Now()
	a.responseRecorder.requestTime = finishTime.Sub(startTime)

	newReplacer := NewReplacer(r, a.responseRecorder, "-")
	response := newReplacer.Replace(a.format)

	helper.AccessLogger.Println(5, response)
}

func NewAccessLogHandler(handler http.Handler, objectLayer ObjectLayer) http.Handler {
	return AccessLogHandler.SetHandler(AccessLogHandler{}, handler, objectLayer)
}
