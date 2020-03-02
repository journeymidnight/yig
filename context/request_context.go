package context

import (
	"io"
	"net/http"

	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/signature"
)

type RequestContextKeyType string

const RequestContextKey RequestContextKeyType = "RequestContext"

type RequestIdKeyType string

const RequestIdKey RequestIdKeyType = "RequestID"

type ContextLoggerKeyType string

const ContextLoggerKey ContextLoggerKeyType = "ContextLogger"

type RequestContext struct {
	RequestID      string
	Logger         log.Logger
	BucketName     string
	ObjectName     string
	BucketInfo     *types.Bucket
	ObjectInfo     *types.Object
	AuthType       signature.AuthType
	IsBucketDomain bool
	Body           io.ReadCloser
	FormValues     map[string]string
}

func GetRequestContext(r *http.Request) RequestContext {
	ctx, ok := r.Context().Value(RequestContextKey).(RequestContext)
	if ok {
		return ctx
	}
	return RequestContext{
		Logger:    r.Context().Value(ContextLoggerKey).(log.Logger),
		RequestID: r.Context().Value(RequestIdKey).(string),
	}
}
