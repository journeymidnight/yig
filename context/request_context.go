package context

import (
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"strings"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta"
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

func (reqCtx *RequestContext) FillBucketAndObjectInfo(r *http.Request, meta *meta.Meta) error {
	var err error
	v := strings.Split(r.Host, ":")
	hostWithOutPort := v[0]
	reqCtx.IsBucketDomain, reqCtx.BucketName = helper.HasBucketInDomain(hostWithOutPort, ".", helper.CONFIG.S3Domain)
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
			reqCtx.ObjectInfo, err = meta.GetObject(reqCtx.BucketInfo.Name, reqCtx.ObjectName, true)
			if err != nil && err != ErrNoSuchKey {
				return err
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
