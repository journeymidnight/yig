package api

import (
	"io"
	"net/http"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/policy"
	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/iam/common"
)

func (api ObjectAPIHandlers) PutBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpPutBucketEncryption)
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

	encryptionConfig, err := datatype.ParseEncryptionConfig(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Unable to parse encryption config:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	err = api.ObjectAPI.SetBucketEncryption(reqCtx.BucketInfo, *encryptionConfig)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Unable to set encryption for bucket:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	WriteSuccessResponse(w, r, nil)

}

func (api ObjectAPIHandlers) GetBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpGetBucketEncryption)
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

	bucketEncryption, err := api.ObjectAPI.GetBucketEncryption(reqCtx.BucketName)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Unable to get encryption from bucket:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	encodedSuccessResponse, err := xmlFormat(bucketEncryption)
	if err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Failed to marshal Encryption XML for bucket:", reqCtx.BucketName, "error:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	setXmlHeader(w)

	// Write to client.
	WriteSuccessResponse(w, r, encodedSuccessResponse)
}

func (api ObjectAPIHandlers) DeleteBucketEncryptionHandler(w http.ResponseWriter, r *http.Request) {
	SetOperationName(w, OpDeleteBucketEncryption)
	reqCtx := GetRequestContext(r)
	logger := reqCtx.Logger

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

	if err := api.ObjectAPI.DeleteBucketEncryption(reqCtx.BucketInfo); err != nil {
		e, logLevel := ParseError(err)
		logger.Log(logLevel, "Unable to delete encryption for bucket:", err)
		WriteErrorResponse(w, r, e)
		return
	}

	// Success.
	WriteSuccessNoContent(w)
}
