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
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
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
	w.Header().Set("Accept-Ranges", "bytes")
	h.handler.ServeHTTP(w, r)
}

func SetCommonHeaderHandler(h http.Handler, _ *meta.Meta) http.Handler {
	return commonHeaderHandler{h}
}

type corsHandler struct {
	handler http.Handler
}

func (h corsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	h.handler.ServeHTTP(w, r)
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
	// Skip the first element which is usually '/' and split the rest.
	ctx := getRequestContext(r)
	bucketName, objectName := ctx.BucketName, ctx.ObjectName
	helper.Logger.Println(5, "ServeHTTP", bucketName, objectName)
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
		helper.Debugln("Host:", r.Host, "Path:", r.URL.Path, "Bucket:", bucketName)
		WriteErrorResponse(w, r, ErrMethodNotAllowed)
		return
	}
	h.handler.ServeHTTP(w, r)
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
		if notimplementedBucketResourceNames[name] {
			helper.Logger.Println(20, "Bucket", name, "has not been implemented.")
			return true
		}
	}
	return false
}

// Checks requests for not implemented Object resources
func ignoreNotImplementedObjectResources(req *http.Request) bool {
	for name := range req.URL.Query() {
		if notimplementedObjectResourceNames[name] {
			helper.Logger.Println(20, "Object", name, "has not been implemented.")
			return true
		}
	}
	return false
}

// List of not implemented bucket queries
var notimplementedBucketResourceNames = map[string]bool{
	"logging":        true,
	"notification":   true,
	"replication":    true,
	"tagging":        true,
	"requestPayment": true,
	"website":        true,
}

// List of not implemented object queries
var notimplementedObjectResourceNames = map[string]bool{
	"torrent": true,
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
	requestId := string(helper.GenerateRandomId())
	bucketName, objectName := GetBucketAndObjectInfoFromRequest(r)

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

	ctx := context.WithValue(
		r.Context(),
		RequestContextKey,
		&RequestContext{
			RequestId:  requestId,
			BucketName: bucketName,
			ObjectName: objectName,
			BucketInfo: bucketInfo,
			ObjectInfo: objectInfo,
			AuthType:   authType,
		})
	helper.Logger.Printf(20, "RequestId: %s, BucketName: %s, ObjectName: %s, BucketInfo: %s, ObjectInfo: %s, AuthType: %d",
		requestId, bucketName, objectName, bucketInfo, objectInfo, authType)
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

func GetBucketAndObjectInfoFromRequest(r *http.Request) (bucketName string, objectName string) {
	splits := strings.SplitN(r.URL.Path[1:], "/", 2)
	v := strings.Split(r.Host, ":")
	hostWithOutPort := v[0]
	ok, bucketName := helper.HasBucketInDomain(hostWithOutPort, ".", helper.CONFIG.S3Domain)
	if ok {
		if len(splits) == 1 {
			objectName = splits[0]
		}
	} else {
		if len(splits) == 1 {
			bucketName = splits[0]
		}
		if len(splits) == 2 {
			bucketName = splits[0]
			objectName = splits[1]
		}
	}
	return
}

func getRequestContext(r *http.Request) *RequestContext {
	return r.Context().Value(RequestContextKey).(*RequestContext)
}
