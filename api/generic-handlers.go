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
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/meta"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/signature"

	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
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

	ctx := GetRequestContext(r)
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
	ctx := GetRequestContext(r)
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
		logger.Error("Method Not Allowed.", "Host:", r.Host, "Path:", r.URL.Path, "Bucket:", bucketName)
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
	requestID := string(helper.GenerateRandomId())
	logger := helper.Logger.NewWithRequestID(requestID)
	ctx := context.WithValue(r.Context(), RequestIdKey, requestID)
	ctx = context.WithValue(ctx, ContextLoggerKey, logger)
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
	var reqCtx RequestContext
	requestId := r.Context().Value(RequestIdKey).(string)
	reqCtx.RequestID = requestId

	logger := r.Context().Value(ContextLoggerKey).(log.Logger)
	reqCtx.Logger = logger
	reqCtx.VersionId = helper.Ternary(r.URL.Query().Get("versionId") == "null", types.NullVersion, r.URL.Query().Get("versionId")).(string)
	err := FillBucketAndObjectInfo(&reqCtx, r, h.meta)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	authType := signature.GetRequestAuthType(r)
	if authType == signature.AuthTypeUnknown {
		WriteErrorResponse(w, r, ErrSignatureVersionNotSupported)
		return
	}
	reqCtx.AuthType = authType

	ctx := context.WithValue(r.Context(), RequestContextKey, reqCtx)
	logger.Info("BucketName:", reqCtx.BucketName, "ObjectName:", reqCtx.ObjectName, "BucketExist:",
		reqCtx.BucketInfo != nil, "ObjectExist:", reqCtx.ObjectInfo != nil, "AuthType:", authType, "VersionId:", reqCtx.VersionId)
	h.handler.ServeHTTP(w, r.WithContext(ctx))
}

func FillBucketAndObjectInfo(reqCtx *RequestContext, r *http.Request, meta *meta.Meta) error {
	var err error
	v := strings.Split(r.Host, ":")
	hostWithOutPort := v[0]
	reqCtx.IsBucketDomain, reqCtx.BucketName = helper.HasBucketInDomain(hostWithOutPort, helper.CONFIG.S3Domain)
	splits := strings.SplitN(r.URL.Path[1:], "/", 2)

	if reqCtx.IsBucketDomain {
		reqCtx.ObjectName = r.URL.Path[1:]
	} else {
		if len(splits) == 1 {
			reqCtx.BucketName = splits[0]
		}
		if len(splits) == 2 {
			reqCtx.BucketName = splits[0]
			reqCtx.ObjectName = splits[1]
		}
	}

	if isPostObjectRequest(r) {
		// PostObject Op extract all data from body
		reader, err := r.MultipartReader()
		if err != nil {
			return ErrMalformedPOSTRequest
		}
		reqCtx.Body, reqCtx.FormValues, err = extractHTTPFormValues(reader)
		if err != nil {
			return err
		}
		reqCtx.ObjectName = reqCtx.FormValues["Key"]
	} else {
		reqCtx.Body = r.Body
		reqCtx.FormValues = nil
	}

	if reqCtx.BucketName != "" {
		reqCtx.BucketInfo, err = meta.GetBucket(reqCtx.BucketName, true)
		if err != nil && err != ErrNoSuchBucket {
			return err
		}
		if reqCtx.BucketInfo != nil && reqCtx.ObjectName != "" {
			if reqCtx.BucketInfo.Versioning == datatype.BucketVersioningDisabled {
				if reqCtx.VersionId != "" {
					return ErrInvalidVersioning
				}
				reqCtx.ObjectInfo, err = meta.GetObject(reqCtx.BucketInfo.Name, reqCtx.ObjectName, types.NullVersion, true)
				if err != nil && err != ErrNoSuchKey {
					return err
				}
			} else if reqCtx.BucketInfo.Versioning == datatype.BucketVersioningSuspended &&
				r.Method != http.MethodGet && r.Method != http.MethodHead {
				reqCtx.ObjectInfo, err = meta.GetObject(reqCtx.BucketInfo.Name, reqCtx.ObjectName, types.NullVersion, true)
				if err != nil && err != ErrNoSuchKey {
					return err
				}
			} else {
				reqCtx.ObjectInfo, err = meta.GetObject(reqCtx.BucketInfo.Name, reqCtx.ObjectName, reqCtx.VersionId, true)
				if err != nil && err != ErrNoSuchKey {
					return err
				}
			}
		}
	}
	return nil
}

func extractHTTPFormValues(reader *multipart.Reader) (filePartReader io.ReadCloser,
	formValues map[string]string, err error) {

	formValues = make(map[string]string)
	for {
		var part *multipart.Part
		part, err = reader.NextPart()
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return nil, nil, err
		}

		if part.FormName() != "file" {
			var buffer []byte
			buffer, err = ioutil.ReadAll(part)
			if err != nil {
				return nil, nil, err
			}
			formValues[http.CanonicalHeaderKey(part.FormName())] = string(buffer)
		} else {
			// "All variables within the form are expanded prior to validating
			// the POST policy"
			fileName := part.FileName()
			objectKey, ok := formValues["Key"]
			if !ok {
				return nil, nil, ErrMissingFields
			}
			if strings.Contains(objectKey, "${filename}") {
				formValues["Key"] = strings.Replace(objectKey, "${filename}", fileName, -1)
			}

			filePartReader = part
			// "The file or content must be the last field in the form.
			// Any fields below it are ignored."
			break
		}
	}

	if filePartReader == nil {
		err = ErrEmptyEntity
	}
	return
}

func isPostObjectRequest(r *http.Request) bool {
	return r.Method == http.MethodPost && strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data")
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
