package api

import (
	"fmt"
	"github.com/opentracing/opentracing-go"
	"net/http"
	"strings"
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
	span, ctx := opentracing.StartSpanFromContext(r.Context(), "AccessLogHandler")
	defer span.Finish()

	a.responseRecorder = NewResponseRecorder(w)

	startTime := time.Now()
	a.handler.ServeHTTP(a.responseRecorder, r.WithContext(ctx))
	finishTime := time.Now()
	a.responseRecorder.requestTime = finishTime.Sub(startTime)

	newReplacer := NewReplacer(r, a.responseRecorder, "-")
	response := newReplacer.Replace(a.format)

	helper.AccessLogger.Println(response)
	// send the entries in access logger to message bus.
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
		helper.Logger.Error("Failed to pack", elems, "err:", err)
		return
	}

	sender, err := bus.GetMessageSender()
	if err != nil {
		helper.Logger.Error("Failed to get message bus sender, err:", err)
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
		helper.Logger.Error(
			fmt.Sprintf("Failed to send message [%v] to message bus, err: %v",
				elems, err))
		return
	}
	helper.Logger.Info(fmt.Sprintf("Succeed to send message [%v] to message bus.",
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
