/*
 * Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package api

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/dustin/go-humanize"
	"github.com/gorilla/mux"
	"github.com/journeymidnight/yig/api/datatype/policy"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/signature"
)

const (
	// As per AWS S3 specification, 20KiB policy JSON data is allowed.
	maxBucketPolicySize = 20 * humanize.KiByte

	// Policy configuration file.
	// TODO: Import policy or Export policy?
	//bucketPolicyConfig = "policy.json"
)

// PutBucketPolicyHandler - This HTTP handler stores given bucket policy configuration as per
// https://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html
func (api ObjectAPIHandlers) PutBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	helper.Debugln("PutBucketPolicyHandler", "enter")
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
	if r.ContentLength > maxBucketPolicySize {
		WriteErrorResponse(w, r, ErrEntityTooLarge)
		return
	}

	bucketPolicy, err := policy.ParseConfig(io.LimitReader(r.Body, r.ContentLength), bucket)
	if err != nil {
		WriteErrorResponse(w, r, ErrMalformedPolicy)
		return
	}

	// Version in policy must not be empty
	if bucketPolicy.Version == "" {
		WriteErrorResponse(w, r, ErrMalformedPolicy)
		return
	}

	if err = api.ObjectAPI.SetBucketPolicy(credential, bucket, *bucketPolicy); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// Success.
	WriteSuccessResponse(w, nil)
}

// DeleteBucketPolicyHandler - This HTTP handler removes bucket policy configuration.
func (api ObjectAPIHandlers) DeleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
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

	if err := api.ObjectAPI.DeleteBucketPolicy(credential, bucket); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// Success.
	WriteSuccessResponse(w, nil)
}

// GetBucketPolicyHandler - This HTTP handler returns bucket policy configuration.
func (api ObjectAPIHandlers) GetBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
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
	bucketPolicy, err := api.ObjectAPI.GetBucketPolicy(credential, bucket)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	policyData, err := json.Marshal(bucketPolicy)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	// Write to client.
	WriteSuccessResponse(w, policyData)
}
