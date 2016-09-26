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
	"net/http"
	"strings"

	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/signature"
	mux "github.com/gorilla/mux"
	"git.letv.cn/yig/yig/helper"
	"net"
)

// HandlerFunc - useful to chain different middleware http.Handler
type HandlerFunc func(http.Handler, ObjectLayer) http.Handler

func RegisterHandlers(router *mux.Router, objectLayer ObjectLayer, handlerFns ...HandlerFunc) http.Handler {
	var f http.Handler
	f = router
	for _, hFn := range handlerFns {
		f = hFn(f, objectLayer)
	}
	return f
}

// Adds Cache-Control header
type cacheControlHandler struct {
	handler http.Handler
}

func SetBrowserCacheControlHandler(h http.Handler, _ ObjectLayer) http.Handler {
	return cacheControlHandler{h}
}

func (h cacheControlHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// TODO
	h.handler.ServeHTTP(w, r)
}

type resourceHandler struct {
	handler http.Handler
}

type corsHandler struct {
	handler     http.Handler
	objectLayer ObjectLayer
}

// setCorsHandler handler for CORS (Cross Origin Resource Sharing)
func SetCorsHandler(h http.Handler, objectLayer ObjectLayer) http.Handler {
	return corsHandler{
		handler:     h,
		objectLayer: objectLayer,
	}
}

func (h corsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Vary", "Origin")

	if r.Header.Get("Origin") == "" { // not a CORS request
		h.handler.ServeHTTP(w, r)
		return
	}

	urlSplit := strings.SplitN(r.URL.Path[1:], "/", 2) // "1:" to remove leading slash
	bucketName := urlSplit[0] // assume bucketName is the first part of url path
	helper.Debugln("bucket", bucketName)
	bucket, err := h.objectLayer.GetBucket(bucketName)
	if err != nil {
		WriteErrorResponse(w, r, err, r.URL.Path)
		return
	}

	if r.Method != "OPTIONS" {
		for _, rule := range bucket.CORS.CorsRules {
			if matchedOrigin, matched := rule.MatchSimple(r); matched {
				rule.SetResponseHeaders(w, r, matchedOrigin)
				h.handler.ServeHTTP(w, r)
				return
			}
		}
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	}

	// r.Method == "OPTIONS", i.e CORS preflight
	w.Header().Add("Vary", "Access-Control-Request-Method")
	w.Header().Add("Vary", "Access-Control-Request-Headers")
	for _, rule := range bucket.CORS.CorsRules {
		if matchedOrigin, matched := rule.MatchPreflight(r); matched {
			rule.SetResponseHeaders(w, r, matchedOrigin)
			WriteSuccessResponse(w, nil)
			return
		}
	}

	WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
}

// setIgnoreResourcesHandler -
// Ignore resources handler is wrapper handler used for API request resource validation
// Since we do not support all the S3 queries, it is necessary for us to throw back a
// valid error message indicating that requested feature is not implemented.
func SetIgnoreResourcesHandler(h http.Handler, _ ObjectLayer) http.Handler {
	return resourceHandler{h}
}

// Resource handler ServeHTTP() wrapper
func (h resourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Skip the first element which is usually '/' and split the rest.
	_, port, _:= net.SplitHostPort(helper.Cfg.BindApiAddress)
	HOST_URL := helper.Cfg.S3Domain + ":" +port
	var bucketName, objectName string
	splits := strings.SplitN(r.URL.Path[1:], "/", 2)
	if strings.HasSuffix(r.Host, "."+HOST_URL) {
		bucketName = strings.TrimSuffix(r.Host, "."+HOST_URL)
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

	helper.Logger.Println("ServeHTTP",bucketName,objectName)
	// If bucketName is present and not objectName check for bucket
	// level resource queries.
	if bucketName != "" && objectName == "" {
		if ignoreNotImplementedBucketResources(r) {
			WriteErrorResponse(w, r, ErrNotImplemented, r.URL.Path)
			return
		}
	}
	// If bucketName and objectName are present check for its resource queries.
	if bucketName != "" && objectName != "" {
		if ignoreNotImplementedObjectResources(r) {
			WriteErrorResponse(w, r, ErrNotImplemented, r.URL.Path)
			return
		}
	}
	// A put method on path "/" doesn't make sense, ignore it.
	if r.Method == "PUT" && r.URL.Path == "/" && bucketName == ""{
		WriteErrorResponse(w, r, ErrNotImplemented, r.URL.Path)
		return
	}
	h.handler.ServeHTTP(w, r)
}

// authHandler - handles all the incoming authorization headers and
// validates them if possible.
type AuthHandler struct {
	handler http.Handler
}

// setAuthHandler to validate authorization header for the incoming request.
func SetAuthHandler(h http.Handler, _ ObjectLayer) http.Handler {
	return AuthHandler{h}
}

// handler for validating incoming authorization headers.
func (a AuthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch signature.GetRequestAuthType(r) {
	case signature.AuthTypeUnknown:
		WriteErrorResponse(w, r, ErrSignatureVersionNotSupported, r.URL.Path)
		return
	default:
		// Let top level caller validate for anonymous and known
		// signed requests.
		a.handler.ServeHTTP(w, r)
		return
	}
}

//// helpers

// Checks requests for not implemented Bucket resources
func ignoreNotImplementedBucketResources(req *http.Request) bool {
	for name := range req.URL.Query() {
		if notimplementedBucketResourceNames[name] {
			return true
		}
	}
	return false
}

// Checks requests for not implemented Object resources
func ignoreNotImplementedObjectResources(req *http.Request) bool {
	for name := range req.URL.Query() {
		if notimplementedObjectResourceNames[name] {
			return true
		}
	}
	return false
}

// List of not implemented bucket queries
var notimplementedBucketResourceNames = map[string]bool{
	"lifecycle":      true,
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
	"policy":  true,
}
