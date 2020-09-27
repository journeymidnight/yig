package api

import (
	"net/http"

	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/signature"
)

func (api ObjectAPIHandlers) PutBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger
	lifecycle := reqCtx.Lifecycle

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "PutBucketLifeCycleHandler signature authenticate err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	logger.Info("Setting lifecycle:", *lifecycle)
	err = api.ObjectAPI.SetBucketLifecycle(reqCtx, *lifecycle, credential)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Unable to set lifecycle for bucket:", err)
		WriteErrorResponse(w, r, e)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucketLifeCycle"
	WriteSuccessResponse(w, r, nil)
}

func (api ObjectAPIHandlers) GetBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	switch signature.GetRequestAuthType(r, reqCtx.Brand) {
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			e, logLevel := ParseError(err)
			logger.Log(logLevel, "GetBucketLifeCycleHandler signature authenticate err:", err)
			WriteErrorResponse(w, r, e)
			return
		}
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	}

	lifecycle, err := api.ObjectAPI.GetBucketLifecycle(reqCtx, credential)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Failed to get bucket lifecycle for bucket", reqCtx.BucketName,
			"error:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	if lifecycle.IsEmpty() {
		logger.Info("The bucket does not have LifeCycle configured!")
		WriteErrorResponse(w, r, ErrNoSuchBucketLc)
		return
	}

	lcBuffer, err := xmlFormat(lifecycle)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Failed to marshal lifecycle XML for bucket:", reqCtx.BucketName, "error:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	setXmlHeader(w)
	//ResponseRecorder
	w.(*ResponseRecorder).operationName = "GetBucketLifeCycle"
	WriteSuccessResponse(w, r, lcBuffer)
}

func (api ObjectAPIHandlers) DelBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "DelBucketLifeCycleHandler signature authenticate err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	err = api.ObjectAPI.DelBucketLifecycle(reqCtx, credential)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Failed to delete bucket lifecycle for bucket", err)
		WriteErrorResponse(w, r, e)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "DelBucketLifeCycle"
	WriteSuccessNoContent(w)

}
