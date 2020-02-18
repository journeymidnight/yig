package api

import (
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/signature"
	"io"
	"net/http"
)

func (api ObjectAPIHandlers) PutBucketEncryption(w http.ResponseWriter, r *http.Request) {
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
	case signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		// Not V4
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4:
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
	// Error out if Content-Length is missing.
	// PutBucketPolicy always needs Content-Length.
	if r.ContentLength <= 0 {
		WriteErrorResponse(w, r, ErrMissingContentLength)
		return
	}

	encryptionConfig, err := datatype.ParseEncryptionConfig(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	err = api.ObjectAPI.SetBucketEncryption(ctx.BucketInfo, *encryptionConfig)
	if err != nil {
		logger.Error("Unable to set encryption for bucket:", err)
		WriteErrorResponse(w, r, err)
		return
	}
	WriteSuccessResponse(w, nil)

}
