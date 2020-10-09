package api

import (
	"io"
	"net/http"

	. "github.com/journeymidnight/yig/api/datatype/lifecycle"
	"github.com/journeymidnight/yig/api/datatype/policy"
	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/iam/common"
)

func (api ObjectAPIHandlers) PutBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpPutBucketLifeCycle)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.PutBucketPolicyAction); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	lifecycle, err := ParseLifecycleConfig(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
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

	WriteSuccessResponse(w, r, nil)
}

func (api ObjectAPIHandlers) GetBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpGetBucketLifeCycle)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.GetBucketPolicyAction); err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "GetBucketLifeCycleHandler checkRequestAuth err:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	lifecycle, err := api.ObjectAPI.GetBucketLifecycle(reqCtx, credential)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Failed to get bucket lifecycle for bucket", reqCtx.BucketName, "error:", err)
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

	WriteSuccessResponse(w, r, lcBuffer)
}

func (api ObjectAPIHandlers) DelBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpDelBucketLifeCycle)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.DeleteBucketPolicyAction); err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "DelBucketLifeCycleHandler checkRequestAuth err:", err)
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

	WriteSuccessNoContent(w)

}
