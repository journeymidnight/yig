/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package api

import (
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/signature"
)

// HandlerFunc - useful to chain different middleware http.Handler
type HandlerFunc func(http.Handler, *meta.Meta) http.Handler

func RegisterHandlers(router *mux.Router, metadata *meta.Meta, handlerFns ...HandlerFunc) http.Handler {
	var f http.Handler
	f = router
	for _, hFn := range handlerFns {
		f = hFn(f, metadata)
	}
	return f
}

// Common headers among ALL the requests, including "Server", "Accept-Ranges",
// "Cache-Control" and more to be added
type commonHeaderHandler struct {
	handler http.Handler
}

func (h commonHeaderHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	span, ctx := opentracing.StartSpanFromContext(r.Context(), "commonHeaderHandler")
	defer func() {
		span.Finish()
	}()
	w.Header().Set("Accept-Ranges", "bytes")
	h.handler.ServeHTTP(w, r.WithContext(ctx))
}

func SetCommonHeaderHandler(h http.Handler, _ *meta.Meta) http.Handler {
	return commonHeaderHandler{h}
}

type corsHandler struct {
	handler http.Handler
}

func (h corsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	span, spanCtx := opentracing.StartSpanFromContext(r.Context(), "corsHandler")
	defer func() {
		span.Finish()
	}()
	w.Header().Add("Vary", "Origin")
	origin := r.Header.Get("Origin")

	if InReservedOrigins(origin) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
		w.Header().Set("Access-Control-Allow-Methods", r.Header.Get("Access-Control-Request-Method"))
		w.Header().Set("Access-Control-Expose-Headers", strings.Join(CommonS3ResponseHeaders, ","))
	}

	ctx := getRequestContext(r)
	bucket := ctx.BucketInfo

	// If bucket CORS exists, overwrite the in-reserved CORS Headers
	if bucket != nil {
		for _, rule := range bucket.CORS.CorsRules {
			if rule.OriginMatched(origin) {
				rule.SetResponseHeaders(w, r)
				break
			}
		}
	}

	if r.Method == "OPTIONS" {
		if origin == "" || r.Header.Get("Access-Control-Request-Method") == "" {
			WriteErrorResponse(w, r, ErrInvalidHeader)
			return
		}
		WriteSuccessResponse(w, nil)
		return
	}

	h.handler.ServeHTTP(w, r.WithContext(spanCtx))
	return
}

// setCorsHandler handler for CORS (Cross Origin Resource Sharing)
func SetCorsHandler(h http.Handler, _ *meta.Meta) http.Handler {
	return corsHandler{h}
}

type resourceHandler struct {
	handler http.Handler
}

// Resource handler ServeHTTP() wrapper
func (h resourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	span, spanCtx := opentracing.StartSpanFromContext(r.Context(), "resourceHandler")
	defer func() {
		span.Finish()
	}()
	// Skip the first element which is usually '/' and split the rest.
	ctx := getRequestContext(r)
	logger := ctx.Logger
	bucketName, objectName := ctx.BucketName, ctx.ObjectName
	// If bucketName is present and not objectName check for bucket
	// level resource queries.
	if bucketName != "" && objectName == "" {
		if ignoreNotImplementedBucketResources(r) {
			WriteErrorResponse(w, r, ErrNotImplemented)
			return
		}
	}
	// If bucketName and objectName are present check for its resource queries.
	if bucketName != "" && objectName != "" {
		if ignoreNotImplementedObjectResources(r) {
			WriteErrorResponse(w, r, ErrNotImplemented)
			return
		}
	}
	// A put method on path "/" doesn't make sense, ignore it.
	if r.Method == "PUT" && r.URL.Path == "/" && bucketName == "" {
		logger.Info("Host:", r.Host, "Path:", r.URL.Path, "Bucket:", bucketName)
		WriteErrorResponse(w, r, ErrMethodNotAllowed)
		return
	}
	h.handler.ServeHTTP(w, r.WithContext(spanCtx))
}

// setIgnoreResourcesHandler -
// Ignore resources handler is wrapper handler used for API request resource validation
// Since we do not support all the S3 queries, it is necessary for us to throw back a
// valid error message indicating that requested feature is not implemented.
func SetIgnoreResourcesHandler(h http.Handler, _ *meta.Meta) http.Handler {
	return resourceHandler{h}
}

// Checks requests for not implemented Bucket resources
func ignoreNotImplementedBucketResources(req *http.Request) bool {
	for name := range req.URL.Query() {
		if notImplementedBucketResourceNames[name] {
			helper.Logger.Warn("Bucket", name, "has not been implemented.")
			return true
		}
	}
	return false
}

// Checks requests for not implemented Object resources
func ignoreNotImplementedObjectResources(req *http.Request) bool {
	for name := range req.URL.Query() {
		if notImplementedObjectResourceNames[name] {
			helper.Logger.Warn("Object", name, "has not been implemented.")
			return true
		}
	}
	return false
}

// List of not implemented bucket queries
var notImplementedBucketResourceNames = map[string]bool{
	"logging":        true,
	"notification":   true,
	"replication":    true,
	"tagging":        true,
	"requestPayment": true,
}

// List of not implemented object queries
var notImplementedObjectResourceNames = map[string]bool{
	"torrent": true,
}

func ContextLogger(r *http.Request) log.Logger {
	return r.Context().Value(RequestContextKey).(RequestContext).Logger
}

type RequestIdHandler struct {
	handler http.Handler
}

func (h RequestIdHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	span, ctx := opentracing.StartSpanFromContext(r.Context(), "RequestIdHandler")
	defer func() {
		span.Finish()
	}()
	requestID := string(helper.GenerateRandomId())
	logger := helper.Logger.NewWithRequestID(requestID)
	ctx = context.WithValue(ctx, RequestIdKey, requestID)
	ctx = context.WithValue(ctx, ContextLoggerKey, logger)
	helper.TracerLogger.For(ctx).TracerInfo("HTTP request ID", zap.String("requestID", requestID),
		zap.String("method", r.Method), zap.String("host", r.Host), zap.String("RequestUrl", r.RequestURI))
	h.handler.ServeHTTP(w, r.WithContext(ctx))
}

func SetRequestIdHandler(h http.Handler, _ *meta.Meta) http.Handler {
	return RequestIdHandler{h}
}

// authHandler - handles all the incoming authorization headers and
// validates them if possible.
type GenerateContextHandler struct {
	handler http.Handler
	meta    *meta.Meta
}

// handler for validating incoming authorization headers.
func (h GenerateContextHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var bucketInfo *types.Bucket
	var objectInfo *types.Object
	var err error
	span, ctx := opentracing.StartSpanFromContext(r.Context(), "GenerateContextHandler")
	defer func() {
		span.Finish()
	}()
	helper.TracerLogger.For(ctx).TracerInfo("HTTP request received", zap.String("method", r.Method))
	requestId := r.Context().Value(RequestIdKey).(string)
	logger := r.Context().Value(ContextLoggerKey).(log.Logger)
	bucketName, objectName, isBucketDomain := GetBucketAndObjectInfoFromRequest(r)
	if bucketName != "" {
		bucketInfo, err = h.meta.GetBucket(bucketName, true)
		if err != nil && err != ErrNoSuchBucket {
			WriteErrorResponse(w, r, err)
			return
		}
		if bucketInfo != nil && objectName != "" {
			objectInfo, err = h.meta.GetObject(bucketInfo.Name, objectName, true)
			if err != nil && err != ErrNoSuchKey {
				WriteErrorResponse(w, r, err)
				return
			}
		}
	}

	authType := signature.GetRequestAuthType(r)
	if authType == signature.AuthTypeUnknown {
		WriteErrorResponse(w, r, ErrSignatureVersionNotSupported)
		return
	}

	ctx = context.WithValue(
		ctx,
		RequestContextKey,
		RequestContext{
			RequestID:      requestId,
			Logger:         logger,
			BucketName:     bucketName,
			ObjectName:     objectName,
			BucketInfo:     bucketInfo,
			ObjectInfo:     objectInfo,
			AuthType:       authType,
			IsBucketDomain: isBucketDomain,
		})
	logger.Info(fmt.Sprintf("BucketName: %s, ObjectName: %s, BucketInfo: %+v, ObjectInfo: %+v, AuthType: %d",
		bucketName, objectName, bucketInfo, objectInfo, authType))
	h.handler.ServeHTTP(w, r.WithContext(ctx))
}

// setAuthHandler to validate authorization header for the incoming request.
func SetGenerateContextHandler(h http.Handler, meta *meta.Meta) http.Handler {
	return GenerateContextHandler{h, meta}
}

func InReservedOrigins(origin string) bool {
	if len(helper.CONFIG.ReservedOrigins) == 0 {
		return false
	}
	OriginsSplit := strings.Split(helper.CONFIG.ReservedOrigins, ",")
	for _, r := range OriginsSplit {
		if strings.Contains(origin, r) {
			return true
		}
	}
	return false
}

//// helpers

func GetBucketAndObjectInfoFromRequest(r *http.Request) (bucketName string, objectName string, isBucketDomain bool) {
	splits := strings.SplitN(r.URL.Path[1:], "/", 2)
	v := strings.Split(r.Host, ":")
	hostWithOutPort := v[0]
	isBucketDomain, bucketName = helper.HasBucketInDomain(hostWithOutPort, ".", helper.CONFIG.S3Domain)
	if isBucketDomain {
		objectName = r.URL.Path[1:]
	} else {
		if len(splits) == 1 {
			bucketName = splits[0]
		}
		if len(splits) == 2 {
			bucketName = splits[0]
			objectName = splits[1]
		}
	}
	return bucketName, objectName, isBucketDomain
}

func getRequestContext(r *http.Request) RequestContext {
	ctx, ok := r.Context().Value(RequestContextKey).(RequestContext)
	if ok {
		return ctx
	}
	return RequestContext{
		Logger:    r.Context().Value(ContextLoggerKey).(log.Logger),
		RequestID: r.Context().Value(RequestIdKey).(string),
	}
}
