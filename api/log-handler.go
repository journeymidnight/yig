package api

import (
	"git.letv.cn/yig/yig/helper"
	"net/http"
	"github.com/labstack/echo/log"
)

type logHandler struct {
	handler http.Handler
}

var Logger log.Logger

func (l logHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Serves the request.
	helper.Debugln(r.Method, r.Host, r.URL)
	Logger.Printf("STARTING %s %s host: %s", r.Method, r.URL, r.Host)
	l.handler.ServeHTTP(w, r)
	Logger.Printf("COMPLETE %s %s host: %s", r.Method, r.URL, r.Host)
}

func SetLogHandler(handler http.Handler) http.Handler {
	return logHandler{handler: handler}
}
