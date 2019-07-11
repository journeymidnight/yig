package api

import (
	"net/http"
	"time"

	"github.com/journeymidnight/yig/helper"
	bus "github.com/journeymidnight/yig/messagebus"
	"github.com/journeymidnight/yig/messagebus/types"
	"github.com/journeymidnight/yig/meta"
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

	helper.AccessLogger.Println(5, response)
	// send the entrys in access logger to message bus.
	elems := newReplacer.GetReplacedValues()
	a.notify(elems)
}

func (a AccessLogHandler) notify(elems map[string]string) {
	if !helper.CONFIG.MsgBus.Enabled {
		return
	}
	if len(elems) == 0 {
		return
	}
	val, err := helper.MsgPackMarshal(elems)
	if err != nil {
		helper.Logger.Printf(2, "failed to pack %v, err: %v", elems, err)
		return
	}

	sender, err := bus.GetMessageSender()
	if err != nil {
		helper.Logger.Printf(2, "failed to get message bus sender, err: %v", err)
		return
	}

	// send the message to message bus async.
	// don't set the ErrChan.
	msg := &types.Message{
		Topic:   helper.CONFIG.MsgBus.Topic,
		Key:     "",
		ErrChan: nil,
		Value:   val,
	}

	err = sender.AsyncSend(msg)
	if err != nil {
		helper.Logger.Printf(2, "failed to send message [%v] to message bus, err: %v", elems, err)
		return
	}
	helper.Logger.Printf(20, "succeed to send message [%v] to message bus.", elems)
}

func NewAccessLogHandler(handler http.Handler, _ *meta.Meta) http.Handler {
	return AccessLogHandler{
		handler: handler,
		format:  CombinedLogFormat,
	}
}
