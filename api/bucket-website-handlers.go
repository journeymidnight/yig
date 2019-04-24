package api

import (
	"github.com/dustin/go-humanize"
	"github.com/gorilla/mux"
	. "github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/signature"
	"io"
	"net/http"
)

const (
	MaxBucketWebsiteConfigurationSize = 20 * humanize.KiByte
)

func (api ObjectAPIHandlers) PutBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	helper.Debugln("PutBucketWebsiteHandler", "enter")
	vars := mux.Vars(r)
	bucket := vars["bucket"]

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

	// Error out if Content-Length is missing.
	// PutBucketPolicy always needs Content-Length.
	if r.ContentLength <= 0 {
		WriteErrorResponse(w, r, ErrMissingContentLength)
		return
	}

	// Error out if Content-Length is beyond allowed size.
	if r.ContentLength > MaxBucketWebsiteConfigurationSize {
		WriteErrorResponse(w, r, ErrEntityTooLarge)
		return
	}

	websiteConfig, err := ParseWebsiteConfig(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	err = api.ObjectAPI.SetBucketWebsite(credential, bucket, *websiteConfig)
	if err != nil {
		helper.ErrorIf(err, "Unable to set website for bucket.")
		WriteErrorResponse(w, r, err)
		return
	}
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) GetBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	helper.Debugln("GetBucketPolicyHandler", "enter")
	vars := mux.Vars(r)
	bucket := vars["bucket"]
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

	// Read bucket access policy.
	bucketWebsite, err := api.ObjectAPI.GetBucketWebsite(credential, bucket)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	encodedSuccessResponse, err := xmlFormat(bucketWebsite)
	if err != nil {
		helper.ErrorIf(err, "Failed to marshal Website XML for bucket", bucket)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w)
	// Write to client.
	WriteSuccessResponse(w, encodedSuccessResponse)
}

func (api ObjectAPIHandlers) DeleteBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
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

	if err := api.ObjectAPI.DeleteBucketWebsite(credential, bucket); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	// Success.
	WriteSuccessNoContent(w)
}
