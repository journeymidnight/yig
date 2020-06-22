package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/journeymidnight/yig/meta/common"

	. "github.com/journeymidnight/yig/context"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta"
	bus "github.com/journeymidnight/yig/mq"
)

type UnexpiredTriple struct {
	StorageClass common.StorageClass
	Size         int64
	SurvivalTime int64 //Nano seconds
}

type ResponseRecorder struct {
	http.ResponseWriter
	status        int
	size          int64
	operationName Operation
	serverCost    time.Duration
	requestTime   time.Duration
	errorCode     string

	targetStorageClass string
	bucketLogging      bool
	cdn_request        bool

	// StorageClass -> deltaSize
	deltaSizeInfo map[common.StorageClass]int64
	// record unexpired STANDARD_IA and GLACIER infos when handle DeleteObjects
	unexpiredObjectsInfo []UnexpiredTriple
}

const timeLayoutStr = "2006-01-02 15:04:05"

func NewResponseRecorder(w http.ResponseWriter) *ResponseRecorder {
	return &ResponseRecorder{
		ResponseWriter: w,
		status:         http.StatusOK,
		deltaSizeInfo:  make(map[common.StorageClass]int64),
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
	ctx := GetRequestContext(r)
	if ctx.ObjectInfo != nil {
		objectLastModifiedTime := ctx.ObjectInfo.LastModifiedTime.Format(timeLayoutStr)
		elems["last_modified_time"] = objectLastModifiedTime
	}
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

func SetOperationName(w http.ResponseWriter, name Operation) {
	if w, ok := w.(*ResponseRecorder); ok {
		w.operationName = name
	}
}

const (
	MinStandardIaObjectSize int64 = 1 << 16 // 64KB
	MinGlacierObjectSize    int64 = 1 << 17 // 128KB
)

// For billing now. FIXME later?
func CorrectDeltaSize(storageClass common.StorageClass, deltaSize int64) (delta int64) {
	var isNagative bool
	if deltaSize < 0 {
		deltaSize *= -1
		isNagative = true
	}
	if storageClass == common.ObjectStorageClassStandardIa && deltaSize < MinStandardIaObjectSize {
		deltaSize = MinStandardIaObjectSize
	} else if storageClass == common.ObjectStorageClassGlacier && deltaSize < MinGlacierObjectSize {
		deltaSize = MinGlacierObjectSize
	}
	delta = helper.Ternary(isNagative, -deltaSize, deltaSize).(int64)
	return
}

func SetDeltaSize(w http.ResponseWriter, storageClass common.StorageClass, delta int64) {
	if w, ok := w.(*ResponseRecorder); ok {
		w.deltaSizeInfo[storageClass] = CorrectDeltaSize(storageClass, delta)
	}
}

func SetUnexpiredInfo(w http.ResponseWriter, info []UnexpiredTriple) {
	if len(info) == 0 {
		return
	}
	if w, ok := w.(*ResponseRecorder); ok {
		w.unexpiredObjectsInfo = info
	}
}
