package api

import (
	"github.com/opentracing/opentracing-go"
	"net/http"

	"github.com/journeymidnight/yig/meta"
)

type logHandler struct {
	handler http.Handler
}

func (l logHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	span, ctx := opentracing.StartSpanFromContext(r.Context(), "logHandler")
	defer span.Finish()
	// Serves the request.
	logger := ContextLogger(r)
	logger.Info("Start serving", r.Method, r.Host, r.URL)
	l.handler.ServeHTTP(w, r.WithContext(ctx))
	logger.Info("Completed", r.Method, r.Host, r.URL)
}

func SetLogHandler(h http.Handler, _ *meta.Meta) http.Handler {
	return logHandler{handler: h}
}
