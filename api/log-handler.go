package api

import (
	"context"
	"github.com/journeymidnight/yig/helper"
	"net/http"
)

type logHandler struct {
	handler http.Handler
}

func (l logHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Serves the request.
	requestId := string(helper.GenerateRandomId())
	ctx := context.WithValue(r.Context(), RequestId, requestId)
	helper.Logger.Printf(5, "STARTING %s %s%s RequestID:%s", r.Method, r.Host, r.URL, requestId)
	l.handler.ServeHTTP(w, r.WithContext(ctx))
	helper.Logger.Printf(5, "COMPLETED %s %s%s RequestID:%s", r.Method, r.Host, r.URL, requestId)
}

func SetLogHandler(handler http.Handler, _ ObjectLayer) http.Handler {
	return logHandler{handler: handler}
}
