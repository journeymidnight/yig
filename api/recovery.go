package api

import (
"git.letv.cn/yig/yig/helper"
"net/http"
)


type recoveryHandler struct {
	handler http.Handler
}

func (l recoveryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			requestId := w.Header().Get("X-Amz-Request-Id")
			helper.Logger.Printf("[PANIC] %s %s%s RequestID:%s", r.Method, r.Host, r.URL, requestId)
			w.WriteHeader(500)
		}
	}()
	l.handler.ServeHTTP(w, r)

}

func SetRecoveryHandler(handler http.Handler, _ ObjectLayer) http.Handler {
	return recoveryHandler{handler: handler}
}
