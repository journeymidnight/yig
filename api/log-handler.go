package api

import (
	"git.letv.cn/yig/yig/helper"
	"net/http"
)

type logHandler struct {
	handler http.Handler
}

func (l logHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Serves the request.
	helper.Debugln(r.Method, r.Host, r.URL)
	helper.Logger.Printf("STARTING %s %s host: %s", r.Method, r.URL, r.Host)
	l.handler.ServeHTTP(w, r)
	helper.Logger.Printf("COMPLETE %s %s host: %s", r.Method, r.URL, r.Host)
}

func SetLogHandler(handler http.Handler) http.Handler {
	return logHandler{handler: handler}
}
