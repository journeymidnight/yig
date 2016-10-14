package api

import (
	"git.letv.cn/yig/yig/helper"
	"math/rand"
	"net/http"
)

// Static alphaNumeric table used for generating unique request ids
var alphaNumericTable = []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

func GenerateRandomId() []byte {
	alpha := make([]byte, 16, 16)
	for i := 0; i < 16; i++ {
		n := rand.Intn(len(alphaNumericTable))
		alpha[i] = alphaNumericTable[n]
	}
	return alpha
}

type logHandler struct {
	handler http.Handler
}

func (l logHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Serves the request.
	helper.Debugln(r.Method, r.Host, r.URL)
	requestId := string(GenerateRandomId())
	helper.Logger.Printf("STARTING %s %s%s RequestID:%s", r.Method, r.Host, r.URL, requestId)
	w.Header().Set("X-Amz-Request-Id", requestId)
	l.handler.ServeHTTP(w, r)
	helper.Logger.Printf("COMPLETED %s %s%s RequestID:%s", r.Method, r.Host, r.URL, requestId)
}

func SetLogHandler(handler http.Handler, _ ObjectLayer) http.Handler {
	return logHandler{handler: handler}
}
