package api

import (
	"io"
	"net/http"
	"strings"

	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/policy"
	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/signature"
)

func (api ObjectAPIHandlers) PutBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpPutBucketWebsite)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.PutBucketPolicyAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	if reqCtx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return
	}
	if credential.ExternRootId != reqCtx.BucketInfo.OwnerId {
		WriteErrorResponse(w, r, ErrBucketAccessForbidden)
		return
	}
	// Error out if Content-Length is missing.
	// PutBucketPolicy always needs Content-Length.
	if r.ContentLength <= 0 {
		WriteErrorResponse(w, r, ErrMissingContentLength)
		return
	}

	websiteConfig, err := ParseWebsiteConfig(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	err = api.ObjectAPI.SetBucketWebsite(reqCtx.BucketInfo, *websiteConfig)
	if err != nil {
		logger.Error("Unable to set website for bucket:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	WriteSuccessResponse(w, r, nil)
}

func (api ObjectAPIHandlers) GetBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpGetBucketWebsite)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.GetBucketPolicyAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	if reqCtx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return
	}
	if credential.ExternRootId != reqCtx.BucketInfo.OwnerId {
		WriteErrorResponse(w, r, ErrBucketAccessForbidden)
		return
	}

	// Read bucket access policy.
	bucketWebsite, err := api.ObjectAPI.GetBucketWebsite(reqCtx.BucketName)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	encodedSuccessResponse, err := xmlFormat(bucketWebsite)
	if err != nil {
		logger.Error("Failed to marshal Website XML for bucket", reqCtx.BucketName,
			"error:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w)
	// Write to client.
	WriteSuccessResponse(w, r, encodedSuccessResponse)
}

func (api ObjectAPIHandlers) DeleteBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpDeleteBucketWebsite)
	reqCtx := GetRequestContext(r)

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.DeleteBucketPolicyAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	if reqCtx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return
	}
	if credential.ExternRootId != reqCtx.BucketInfo.OwnerId {
		WriteErrorResponse(w, r, ErrBucketAccessForbidden)
		return
	}

	if err := api.ObjectAPI.DeleteBucketWebsite(reqCtx.BucketInfo); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	// Success.
	WriteSuccessNoContent(w)
}

func (api ObjectAPIHandlers) HandledByWebsite(w http.ResponseWriter, r *http.Request) (handled bool) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	if reqCtx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return true
	}
	if reqCtx.AuthType != signature.AuthTypeAnonymous {
		return false
	}

	website := reqCtx.BucketInfo.Website
	// redirect
	if redirect := website.RedirectAllRequestsTo; redirect != nil && redirect.HostName != "" {
		if !reqCtx.IsBucketDomain {
			WriteErrorResponse(w, r, ErrSecondLevelDomainForbidden)
			return true
		}
		protocol := redirect.Protocol
		if protocol == "" {
			protocol = helper.Ternary(r.URL.Scheme == "", "http", r.URL.Scheme).(string)
		}
		http.Redirect(w, r, protocol+"://"+redirect.HostName+r.RequestURI, http.StatusFound)
		return true
	}

	if id := website.IndexDocument; id != nil && id.Suffix != "" {
		if !reqCtx.IsBucketDomain {
			WriteErrorResponse(w, r, ErrSecondLevelDomainForbidden)
			return true
		}

		// match routing rules
		if len(website.RoutingRules) != 0 {
			for _, rule := range website.RoutingRules {
				// If the condition matches, handle redirect
				if rule.Match(reqCtx.ObjectName, "") {
					rule.DoRedirect(w, r, reqCtx.ObjectName)
					return true
				}
			}
		}

		// handle IndexDocument
		if strings.HasSuffix(reqCtx.ObjectName, "/") || reqCtx.ObjectName == "" {
			indexName := reqCtx.ObjectName + id.Suffix
			credential := common.Credential{}
			isAllow, err := IsBucketPolicyAllowed(&credential, reqCtx.BucketInfo, r, policy.GetObjectAction, indexName)
			if err != nil {
				WriteErrorResponse(w, r, err)
				return true
			}
			credential.AllowOtherUserAccess = isAllow
			index, err := api.ObjectAPI.GetObjectInfo(reqCtx.BucketName, indexName, "", credential)
			if err != nil {
				if err == ErrNoSuchKey {
					api.errAllowableObjectNotFound(w, r, credential)
					return true
				}
				WriteErrorResponse(w, r, err)
				return true
			}
			writer := newGetObjectResponseWriter(w, r, index, nil, http.StatusOK, "", reqCtx.AuthType)
			// Reads the object at startOffset and writes to mw.
			if err := api.ObjectAPI.GetObject(index, 0, index.Size, writer, datatype.SseRequest{}); err != nil {
				logger.Error("Unable to write to client:", err)
				if !writer.dataWritten {
					// Error response only if no data has been written to client yet. i.e if
					// partial data has already been written before an error
					// occurred then no point in setting StatusCode and
					// sending error XML.
					WriteErrorResponse(w, r, err)
				}
				return true
			}
			if !writer.dataWritten {
				// If ObjectAPI.GetObject did not return error and no data has
				// been written it would mean that it is a 0-byte object.
				// call wrter.Write(nil) to set appropriate headers.
				writer.Write(nil)
			}
			return true
		}

	}
	return false
}

func (api ObjectAPIHandlers) ReturnWebsiteErrorDocument(w http.ResponseWriter, r *http.Request, statusCode int) (handled bool) {
	w.(*ResponseRecorder).operationName = "GetObject"
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	if reqCtx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return true
	}
	website := reqCtx.BucketInfo.Website
	if ed := website.ErrorDocument; ed != nil && ed.Key != "" {
		indexName := ed.Key
		credential := common.Credential{}
		isAllow, err := IsBucketPolicyAllowed(&credential, reqCtx.BucketInfo, r, policy.GetObjectAction, indexName)
		if err != nil {
			WriteErrorResponse(w, r, err)
			return true
		}
		credential.AllowOtherUserAccess = isAllow
		index, err := api.ObjectAPI.GetObjectInfo(reqCtx.BucketName, indexName, "", credential)
		if err != nil {
			WriteErrorResponse(w, r, err)
			return true
		}
		writer := newGetObjectResponseWriter(w, r, index, nil, http.StatusNotFound, "", reqCtx.AuthType)
		// Reads the object at startOffset and writes to mw.
		if err := api.ObjectAPI.GetObject(index, 0, index.Size, writer, datatype.SseRequest{}); err != nil {
			logger.Error("Unable to write to client:", err)
			if !writer.dataWritten {
				// Error response only if no data has been written to client yet. i.e if
				// partial data has already been written before an error
				// occurred then no point in setting StatusCode and
				// sending error XML.
				WriteErrorResponse(w, r, err)
			}
			return true
		}
		if !writer.dataWritten {
			// If ObjectAPI.GetObject did not return error and no data has
			// been written it would mean that it is a 0-byte object.
			// call wrter.Write(nil) to set appropriate headers.
			writer.Write(nil)
		}
		return true
	} else {
		return false
	}
}
