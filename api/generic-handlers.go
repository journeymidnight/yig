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
	"time"

	"github.com/bsm/redislock"
	"github.com/gorilla/mux"
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/backend"
	. "github.com/journeymidnight/yig/brand"
	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
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
	ctx := GetRequestContext(r)
	w.Header().Add("Vary", "Origin")
	origin := r.Header.Get("Origin")

	if InReservedOrigins(origin) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
		w.Header().Set("Access-Control-Allow-Methods", r.Header.Get("Access-Control-Request-Method"))
		w.Header().Set("Access-Control-Expose-Headers", strings.Join(CommonS3ResponseHeaders, ","))
	}

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
		WriteSuccessResponse(w, r, nil)
		return
	}

	h.handler.ServeHTTP(w, r)
	return
}

// setCorsHandler handler for CORS (Cross Origin Resource Sharing)
func SetCorsHandler(h http.Handler, _ *meta.Meta) http.Handler {
	return corsHandler{h}
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

	brand := DistinguishBrandName(r, reqCtx.FormValues)
	reqCtx.Brand = brand

	authType := signature.GetRequestAuthType(r, brand)
	if authType == signature.AuthTypeUnknown {
		WriteErrorResponse(w, r, ErrSignatureVersionNotSupported)
		return
	}
	reqCtx.AuthType = authType
	reqCtx.Mutex = nil

	logger.Info("BucketName:", reqCtx.BucketName, "ObjectName:", reqCtx.ObjectName, "BucketExist:",
		reqCtx.BucketInfo != nil, "ObjectExist:", reqCtx.ObjectInfo != nil, "AuthType:", authType, "VersionId:", reqCtx.VersionId)
	//if it is a modification operation to a appendable object, lock it
	if reqCtx.ObjectInfo != nil {
		if reqCtx.ObjectInfo.Type == types.ObjectTypeAppendable && reqCtx.ObjectInfo.Pool == backend.SMALL_FILE_POOLNAME &&
			(r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodDelete) {
			// as this request is sent to ssd, 5 seconds should be enough
			reqCtx.Mutex, err = redis.Locker.Obtain(redis.GenMutexKey(reqCtx.ObjectInfo), 5*time.Second, nil)
			if err == redislock.ErrNotObtained {
				helper.Logger.Error("Lock object failed:", reqCtx.ObjectInfo.BucketName, reqCtx.ObjectInfo.ObjectId, reqCtx.ObjectInfo.VersionId)
				WriteErrorResponse(w, r, ErrObjectMutexProtected)
				return
			} else if err != nil {
				helper.Logger.Error("Lock seems does not work, check redis config and aliveness, but continue this request", err.Error())
			} else {
				helper.Logger.Info("Lock object success", reqCtx.ObjectInfo.BucketName, reqCtx.ObjectName, reqCtx.ObjectInfo.ObjectId, reqCtx.ObjectInfo.VersionId)
			}

		}
	}

	ctx := context.WithValue(r.Context(), RequestContextKey, reqCtx)
	ctx = context.WithValue(ctx, BrandKey, brand)
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
				r.Method != http.MethodGet && r.Method != http.MethodHead && reqCtx.VersionId == "" {
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

type QosHandler struct {
	handler http.Handler
	meta    *meta.Meta
}

func (h *QosHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := GetRequestContext(r)
	if len(ctx.BucketName) == 0 {
		h.handler.ServeHTTP(w, r)
		return
	}
	var allow bool
	if r.Method == "GET" || r.Method == "HEAD" { // read operations
		allow = h.meta.QosMeta.AllowReadQuery(ctx.BucketName)
	} else { // write operations
		allow = h.meta.QosMeta.AllowWriteQuery(ctx.BucketName)
	}
	if !allow {
		WriteErrorResponse(w, r, ErrRequestLimitExceeded)
		return
	}
	h.handler.ServeHTTP(w, r)
}

func SetQosHandler(h http.Handler, meta *meta.Meta) http.Handler {
	qos := QosHandler{
		handler: h,
		meta:    meta,
	}
	return &qos
}
