package context

import (
	"io"
	"net/http"

	"github.com/bsm/redislock"
	"github.com/journeymidnight/yig/api/datatype/lifecycle"
	"github.com/journeymidnight/yig/brand"
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
	RequestID               string
	Logger                  log.Logger
	BucketName              string
	ObjectName              string
	BucketInfo              *types.Bucket
	ObjectInfo              *types.Object
	Lifecycle               *lifecycle.Lifecycle
	BrandType               brand.Brand
	AuthType                signature.AuthType
	IsBucketDomain          bool
	IsObjectForbidOverwrite bool
	Body                    io.ReadCloser
	FormValues              map[string]string
	VersionId               string
	Mutex                   *redislock.Lock
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

func GetContextLogger(r *http.Request) log.Logger {
	return r.Context().Value(RequestContextKey).(RequestContext).Logger
}
