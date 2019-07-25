package api

import (
	"net/http"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta"
)

type logHandler struct {
	handler http.Handler
}

func (l logHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Serves the request.
	requestId := r.Context().Value(RequestContextKey).(RequestContext).RequestId
	helper.Logger.Printf(5, "[ %s ] STARTING %s %s%s RequestID:%s", requestId, r.Method, r.Host, r.URL, requestId)
	l.handler.ServeHTTP(w, r)
	helper.Logger.Printf(5, "[ %s ] COMPLETED %s %s%s RequestID:%s", requestId, r.Method, r.Host, r.URL, requestId)
}

func SetLogHandler(h http.Handler, _ *meta.Meta) http.Handler {
	return logHandler{handler: h}
}
