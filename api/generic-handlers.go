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

	"github.com/gorilla/mux"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/signature"
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

// Common headers among ALL the requests, including "Server", "Accept-Ranges",
// "Cache-Control" and more to be added
type commonHeaderHandler struct {
	handler http.Handler
}

func (h commonHeaderHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Accept-Ranges", "bytes")
	h.handler.ServeHTTP(w, r)
}

func SetCommonHeaderHandler(h http.Handler, _ ObjectLayer) http.Handler {
	return commonHeaderHandler{h}
}

type corsHandler struct {
	handler     http.Handler
	objectLayer ObjectLayer
}

func (h corsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Vary", "Origin")
	origin := r.Header.Get("Origin")
	if r.Method != "OPTIONS" {
		if origin == "" {
			h.handler.ServeHTTP(w, r)
			return
		}
		if origin != "" && InReservedOrigins(origin) {
			w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))
			w.Header().Set("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
			w.Header().Set("Access-Control-Allow-Methods", r.Header.Get("Access-Control-Request-Method"))
			w.Header().Set("Access-Control-Expose-Headers", "*")
			h.handler.ServeHTTP(w, r)
			return
		}
	} else {
		// an OPTIONS request without "Origin" and "Access-Control-Request-Method" set properly
		if r.Header.Get("Origin") == "" || r.Header.Get("Access-Control-Request-Method") == "" {
			WriteErrorResponse(w, r, ErrInvalidHeader)
			return
		}
	}

	if r.Method == "OPTIONS" && InReservedOrigins(origin) {
		w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))
		w.Header().Set("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
		w.Header().Set("Access-Control-Allow-Methods", r.Header.Get("Access-Control-Request-Method"))
		w.Header().Set("Access-Control-Expose-Headers", "*")
		WriteSuccessResponse(w, nil)
		return
	}

	bucketName, _ := GetBucketAndObjectInfoFromRequest(r)
	bucket, err := h.objectLayer.GetBucket(bucketName)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	if r.Method != "OPTIONS" {
		for _, rule := range bucket.CORS.CorsRules {
			if matched := rule.MatchSimple(r); matched {
				rule.SetResponseHeaders(w, r, r.Header.Get("Origin"))
				break
			}
		}
		h.handler.ServeHTTP(w, r)
		return
	}

	// r.Method == "OPTIONS", i.e CORS preflight
	w.Header().Add("Vary", "Access-Control-Request-Method")
	w.Header().Add("Vary", "Access-Control-Request-Headers")
	for _, rule := range bucket.CORS.CorsRules {
		if matched := rule.MatchPreflight(r); matched {
			rule.SetResponseHeaders(w, r, r.Header.Get("Origin"))
			WriteSuccessResponse(w, nil)
			return
		}
	}

	WriteErrorResponse(w, r, ErrAccessDenied)
}

// setCorsHandler handler for CORS (Cross Origin Resource Sharing)
func SetCorsHandler(h http.Handler, o ObjectLayer) http.Handler {
	return corsHandler{
		handler:     h,
		objectLayer: o,
	}
}

type resourceHandler struct {
	handler http.Handler
}

// Resource handler ServeHTTP() wrapper
func (h resourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Skip the first element which is usually '/' and split the rest.
	bucketName, objectName := GetBucketAndObjectInfoFromRequest(r)
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
func SetIgnoreResourcesHandler(h http.Handler, _ ObjectLayer) http.Handler {
	return resourceHandler{h}
}

// authHandler - handles all the incoming authorization headers and
// validates them if possible.
type AuthHandler struct {
	handler http.Handler
}

// handler for validating incoming authorization headers.
func (a AuthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch signature.GetRequestAuthType(r) {
	case signature.AuthTypeUnknown:
		WriteErrorResponse(w, r, ErrSignatureVersionNotSupported)
		return
	default:
		// Let top level caller validate for anonymous and known
		// signed requests.
		a.handler.ServeHTTP(w, r)
		return
	}
}

// setAuthHandler to validate authorization header for the incoming request.
func SetAuthHandler(h http.Handler, _ ObjectLayer) http.Handler {
	return AuthHandler{h}
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

// guessIsBrowserReq - returns true if the request is browser.
// This implementation just validates user-agent and
// looks for "Mozilla" string. This is no way certifiable
// way to know if the request really came from a browser
// since User-Agent's can be arbitrary. But this is just
// a best effort function.
func guessIsBrowserReq(req *http.Request) bool {
	if req == nil {
		return false
	}
	return true
	//return strings.Contains(req.Header.Get("User-Agent"), "Mozilla")
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
