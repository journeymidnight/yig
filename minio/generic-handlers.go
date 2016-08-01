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

package minio

import (
	"net/http"
	"strings"

	. "git.letv.cn/yig/yig/minio/datatype"
	router "github.com/gorilla/mux"
	"github.com/rs/cors"
)

// HandlerFunc - useful to chain different middleware http.Handler
type HandlerFunc func(http.Handler) http.Handler

func registerHandlers(mux *router.Router, handlerFns ...HandlerFunc) http.Handler {
	var f http.Handler
	f = mux
	for _, hFn := range handlerFns {
		f = hFn(f)
	}
	return f
}

// Adds Cache-Control header
type cacheControlHandler struct {
	handler http.Handler
}

func setBrowserCacheControlHandler(h http.Handler) http.Handler {
	return cacheControlHandler{h}
}

func (h cacheControlHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// TODO
	h.handler.ServeHTTP(w, r)
}

type resourceHandler struct {
	handler http.Handler
}

// setCorsHandler handler for CORS (Cross Origin Resource Sharing)
func setCorsHandler(h http.Handler) http.Handler {
	c := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "HEAD", "POST", "PUT"},
		AllowedHeaders: []string{"*"},
		ExposedHeaders: []string{"ETag"},
	})
	return c.Handler(h)
}

// setIgnoreResourcesHandler -
// Ignore resources handler is wrapper handler used for API request resource validation
// Since we do not support all the S3 queries, it is necessary for us to throw back a
// valid error message indicating that requested feature is not implemented.
func setIgnoreResourcesHandler(h http.Handler) http.Handler {
	return resourceHandler{h}
}

// Resource handler ServeHTTP() wrapper
func (h resourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Skip the first element which is usually '/' and split the rest.
	splits := strings.SplitN(r.URL.Path[1:], "/", 2)

	// Save bucketName and objectName extracted from url Path.
	var bucketName, objectName string
	if len(splits) == 1 {
		bucketName = splits[0]
	}
	if len(splits) == 2 {
		bucketName = splits[0]
		objectName = splits[1]
	}
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
	if r.Method == "PUT" && r.URL.Path == "/" {
		WriteErrorResponse(w, r, ErrNotImplemented, r.URL.Path)
		return
	}
	h.handler.ServeHTTP(w, r)
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
	"acl":            true,
	"cors":           true,
	"lifecycle":      true,
	"logging":        true,
	"notification":   true,
	"replication":    true,
	"tagging":        true,
	"versions":       true,
	"requestPayment": true,
	"versioning":     true,
	"website":        true,
}

// List of not implemented object queries
var notimplementedObjectResourceNames = map[string]bool{
	"torrent": true,
	"acl":     true,
	"policy":  true,
}
