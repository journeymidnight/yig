package api

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"

	lc "github.com/journeymidnight/yig/api/datatype/lifecycle"
	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	. "github.com/journeymidnight/yig/meta/common"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta"
)

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
		for name := range r.URL.Query() {
			if ignoreUnsupportedBucketResources(name) {
				WriteErrorResponse(w, r, ErrUnsupportFunction)
				return
			}
			if name == "lifecycle" && r.Method == "PUT" {
				lifecycle, isUnsupportedLifecycleXml, err := ignoreLifecycleUnsupported(r)
				if err != nil {
					helper.Logger.Error("Unable to parse lifecycle body:", err)
					WriteErrorResponse(w, r, err)
					return
				}
				if isUnsupportedLifecycleXml {
					WriteErrorResponse(w, r, ErrUnsupportFunction)
					return
				}
				ctx.Lifecycle = lifecycle
			}
		}
	}
	// If bucketName and objectName are present check for its resource queries.
	if bucketName != "" && objectName != "" {
		if ignoreUnsupportedObjectResources(r) {
			WriteErrorResponse(w, r, ErrUnsupportFunction)
			return
		}
	}

	isUnsupportedStorageClass, err := ignoreUnsupportedStorageClass(r)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	if isUnsupportedStorageClass {
		WriteErrorResponse(w, r, ErrUnsupportFunction)
		return
	}

	// A put method on path "/" doesn't make sense, ignore it.
	if r.Method == "PUT" && r.URL.Path == "/" && bucketName == "" {
		logger.Error("Method Not Allowed.", "Host:", r.Host, "Path:", r.URL.Path, "Bucket:", bucketName)
		WriteErrorResponse(w, r, ErrMethodNotAllowed)
		return
	}

	newctx := context.WithValue(r.Context(), RequestContextKey, ctx)
	h.handler.ServeHTTP(w, r.WithContext(newctx))
}

// setFunctionSwitchHandler -
// Function switch handler is wrapper handler used for API request resource validation
// Since we do not support all the S3 queries, it is necessary for us to throw back a
// valid error message indicating that requested feature is not support.
func SetFunctionSwitchHandler(h http.Handler, _ *meta.Meta) http.Handler {
	initUnsupportedFunctions()
	return resourceHandler{h}
}

// Checks requests for not supported Bucket resources
func ignoreUnsupportedBucketResources(name string) bool {
	if unsupportedBucketResourceNames[name] {
		helper.Logger.Warn("Bucket", name, "has not been implemented.")
		return true
	}
	return false
}

// Checks requests for not supported Object resources
func ignoreUnsupportedObjectResources(req *http.Request) bool {
	for name := range req.URL.Query() {
		if unsupportedObjectResourceNames[name] {
			helper.Logger.Warn("Object", name, "has not been implemented.")
			return true
		}
	}
	return false
}

// Checks requests for not supported Object resources
func ignoreUnsupportedStorageClass(req *http.Request) (bool, error) {
	storageClass, err := getStorageClassFromHeader(req.Header)
	if err != nil {
		return false, err
	}
	if unsupportedStorageClassNames[storageClass] {
		helper.Logger.Warn("StorageClass", storageClass, "has not been supported.")
		return true, nil
	}
	return false, nil
}

//
func ignoreLifecycleUnsupported(r *http.Request) (*lc.Lifecycle, bool, error) {
	var lifecycle *lc.Lifecycle
	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return lifecycle, true, ErrInternalError
	}
	lifecycle, err = lc.ParseLifecycleConfig(payload)
	if err != nil {
		return lifecycle, true, err
	}
	r.Body = ioutil.NopCloser(bytes.NewReader(payload))
	for _, rule := range lifecycle.Rules {
		if unsupportedBucketResourceNames["versioning"] {
			if rule.NoncurrentVersionExpiration != nil || rule.NoncurrentVersionTransitions != nil {
				helper.Logger.Warn("Lifecycle versioning", rule, "has not been supported.")
				return lifecycle, true, nil
			}
		}
		for _, transition := range rule.Transitions {
			storageClass, err := MatchStorageClassIndex(transition.StorageClass)
			if err != nil {
				helper.Logger.Warn("Lifecycle StorageClass", err)
				return lifecycle, true, err
			}
			if unsupportedStorageClassNames[storageClass] {
				helper.Logger.Warn("Lifecycle StorageClass", storageClass, "has not been supported.")
				return lifecycle, true, nil
			}
		}
		for _, noncurrentVersionTransition := range rule.NoncurrentVersionTransitions {
			storageClass, err := MatchStorageClassIndex(noncurrentVersionTransition.StorageClass)
			if err != nil {
				helper.Logger.Warn("Lifecycle StorageClass", err)
				return lifecycle, true, err
			}
			if unsupportedStorageClassNames[storageClass] {
				helper.Logger.Warn("Lifecycle StorageClass", storageClass, "has not been supported.")
				return lifecycle, true, nil
			}
		}
	}
	return lifecycle, false, nil
}

// List of not implemented bucket queries
var unsupportedBucketResourceNames = map[string]bool{}

// List of not implemented object queries
var unsupportedObjectResourceNames = map[string]bool{}

// List of not implemented object storage class queries
var unsupportedStorageClassNames = map[StorageClass]bool{}

func initUnsupportedFunctions() {
	// Initialize features unsupported by the current node bucket
	for _, function := range helper.CONFIG.FuncNotSupportForBucket {
		unsupportedBucketResourceNames[function] = true
	}

	// Initialize functions unsupported by the current node storage object
	for _, function := range helper.CONFIG.FuncNotSupportForObject {
		switch function {
		case "standard_ia":
			unsupportedStorageClassNames[ObjectStorageClassStandardIa] = true
		case "glacier":
			unsupportedStorageClassNames[ObjectStorageClassGlacier] = true
		default:
			unsupportedObjectResourceNames[function] = true
		}
	}
}
