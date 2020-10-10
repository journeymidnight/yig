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
		WriteInternalErrorResponse(w, r, err, "PutBucketEncryptionHandler checkRequestAuth err:")
		return
	}

	lifecycle, err := ParseLifecycleConfig(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		WriteInternalErrorResponse(w, r, err, "Unable to parse lifecycle config:")
		return
	}

	logger.Info("Setting lifecycle:", *lifecycle)
	err = api.ObjectAPI.SetBucketLifecycle(reqCtx, *lifecycle, credential)
	if err != nil {
		WriteInternalErrorResponse(w, r, err, "Unable to set lifecycle for bucket:")
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
		WriteInternalErrorResponse(w, r, err, "GetBucketLifeCycleHandler checkRequestAuth err:")
		return
	}

	lifecycle, err := api.ObjectAPI.GetBucketLifecycle(reqCtx, credential)
	if err != nil {
		WriteInternalErrorResponse(w, r, err, "Failed to get bucket lifecycle for bucket", reqCtx.BucketName, "error:")
		return
	}

	if lifecycle.IsEmpty() {
		logger.Info("The bucket does not have LifeCycle configured!")
		WriteErrorResponse(w, r, ErrNoSuchBucketLc)
		return
	}

	lcBuffer, err := xmlFormat(lifecycle)
	if err != nil {
		WriteInternalErrorResponse(w, r, err, "Failed to marshal lifecycle XML for bucket:", reqCtx.BucketName, "error:")
		return
	}

	setXmlHeader(w)

	WriteSuccessResponse(w, r, lcBuffer)
}

func (api ObjectAPIHandlers) DelBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpDelBucketLifeCycle)
	reqCtx := GetRequestContext(r)

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.DeleteBucketPolicyAction); err != nil {
		WriteInternalErrorResponse(w, r, err, "DelBucketLifeCycleHandler checkRequestAuth err:")
		return
	}

	err = api.ObjectAPI.DelBucketLifecycle(reqCtx, credential)
	if err != nil {
		WriteInternalErrorResponse(w, r, err, "Failed to delete bucket lifecycle for bucket")
		return
	}

	WriteSuccessNoContent(w)

}
