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
	requestId := getRequestContext(r).RequestId
	helper.Logger.Info("STARTING", r.Method, r.Host, r.URL, "RequestID:", requestId)
	l.handler.ServeHTTP(w, r)
	helper.Logger.Info("COMPLETED", r.Method, r.Host, r.URL, "RequestID:", requestId)
}

func SetLogHandler(h http.Handler, _ *meta.Meta) http.Handler {
	return logHandler{handler: h}
}
