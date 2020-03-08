package api

import (
	. "github.com/journeymidnight/yig/api/datatype/lifecycle"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/signature"
	"io"
	"net/http"
)

func (api ObjectAPIHandlers) PutBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx := getRequestContext(r)
	logger := ctx.Logger

	var credential common.Credential
	var err error
	switch ctx.AuthType {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}

	if ctx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return
	}
	if credential.UserId != ctx.BucketInfo.OwnerId {
		WriteErrorResponse(w, r, ErrBucketAccessForbidden)
		return
	}

	lifecycle, err := ParseLifecycleConfig(io.LimitReader(r.Body,r.ContentLength))
	if err != nil {
		logger.Error("Unable to parse lifecycle body:", err)
		WriteErrorResponse(w, r, ErrInvalidLc)
		return
	}

	logger.Info("Setting lifecycle:", *lifecycle)
	err = api.ObjectAPI.SetBucketLifecycle(ctx.BucketInfo, *lifecycle)
	if err != nil {
		logger.Error(err, "Unable to set lifecycle for bucket:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucketLifeCycle"
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) GetBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx := getRequestContext(r)
	logger := ctx.Logger

	var credential common.Credential
	var err error
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}

	if ctx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return
	}
	if credential.UserId != ctx.BucketInfo.OwnerId {
		WriteErrorResponse(w, r, ErrBucketAccessForbidden)
		return
	}

	lifecycle, err := api.ObjectAPI.GetBucketLifecycle(ctx.BucketName)
	if err != nil {
		logger.Error("Failed to get bucket ACL policy for bucket", ctx.BucketName,
			"error:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	lcBuffer, err := xmlFormat(lifecycle)
	if err != nil {
		logger.Error("Failed to marshal lifecycle XML for bucket", ctx.BucketName,
			"error:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w)
	//ResponseRecorder
	w.(*ResponseRecorder).operationName = "GetBucketLifeCycle"
	WriteSuccessResponse(w, lcBuffer)

}

func (api ObjectAPIHandlers) DelBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	ctx := getRequestContext(r)

	var credential common.Credential
	var err error
	switch ctx.AuthType {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}

	if ctx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return
	}
	if credential.UserId != ctx.BucketInfo.OwnerId {
		WriteErrorResponse(w, r, ErrBucketAccessForbidden)
		return
	}

	err = api.ObjectAPI.DelBucketLifecycle(ctx.BucketInfo)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "DelBucketLifeCycle"
	WriteSuccessNoContent(w)

}