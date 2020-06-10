package api

import (
	"io"
	"net/http"

	. "github.com/journeymidnight/yig/api/datatype/lifecycle"
	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/signature"
)

func (api ObjectAPIHandlers) PutBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpPutBucketLifeCycle)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	lifecycle, err := ParseLifecycleConfig(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		logger.Error("Unable to parse lifecycle body:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	logger.Info("Setting lifecycle:", *lifecycle)
	err = api.ObjectAPI.SetBucketLifecycle(reqCtx, *lifecycle, credential)
	if err != nil {
		logger.Error(err, "Unable to set lifecycle for bucket:", err)
		WriteErrorResponse(w, r, err)
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
	switch signature.GetRequestAuthType(r) {
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	}

	lifecycle, err := api.ObjectAPI.GetBucketLifecycle(reqCtx, credential)
	if err != nil {
		logger.Error("Failed to get bucket ACL policy for bucket", reqCtx.BucketName,
			"error:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	if lifecycle.IsEmpty() {
		logger.Info("The bucket does not have LifeCycle configured!")
		WriteErrorResponse(w, r, ErrNoSuchBucketLc)
		return
	}

	lcBuffer, err := xmlFormat(lifecycle)
	if err != nil {
		logger.Error("Failed to marshal lifecycle XML for bucket", reqCtx.BucketName,
			"error:", err)
		WriteErrorResponse(w, r, ErrInternalError)
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
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	err = api.ObjectAPI.DelBucketLifecycle(reqCtx, credential)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	WriteSuccessNoContent(w)

}
