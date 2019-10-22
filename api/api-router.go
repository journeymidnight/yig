/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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
	router "github.com/gorilla/mux"
	"github.com/journeymidnight/yig/helper"
)

// objectAPIHandler implements and provides http handlers for S3 API.
type ObjectAPIHandlers struct {
	ObjectAPI ObjectLayer
}

// registerAPIRouter - registers S3 compatible APIs.
func RegisterAPIRouter(mux *router.Router, api ObjectAPIHandlers) {
	// API Router
	apiRouter := mux.NewRoute().PathPrefix("/").Subrouter()

	var routers []*router.Router
	for _, domain := range helper.CONFIG.S3Domain {
		// Bucket router, matches domain.name/bucket_name/object_name
		bucket := apiRouter.Host(domain).PathPrefix("/{bucket}").Subrouter()
		// Host router, matches bucket_name.domain.name/object_name
		bucket_host := apiRouter.Host("{bucket:.+}." + domain).Subrouter()
		routers = append(routers, bucket, bucket_host)
	}

	for _, bucket := range routers {
		/// Object operations
		// HeadObject
		bucket.Methods("HEAD").Path("/{object:.+}").HandlerFunc(api.HeadObjectHandler)
		// PutObjectPart - Copy
		bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(api.CopyObjectPartHandler).
			Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}").
			HeadersRegexp("X-Amz-Copy-Source", ".*?(/).*?")
		// PutObjectPart
		bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(api.PutObjectPartHandler).
			Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")
		// ListObjectParts
		bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(api.ListObjectPartsHandler).
			Queries("uploadId", "{uploadId:.*}")
		// CompleteMultipartUpload
		bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(api.CompleteMultipartUploadHandler).
			Queries("uploadId", "{uploadId:.*}")
		// NewMultipartUpload
		bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(api.NewMultipartUploadHandler).
			Queries("uploads", "")
		// AbortMultipartUpload
		bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(api.AbortMultipartUploadHandler).
			Queries("uploadId", "{uploadId:.*}")
		// CopyObject
		bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Copy-Source", ".*?(/).*?").
			HandlerFunc(api.CopyObjectHandler)
		// RenameObject
		bucket.Methods("PUT").Path("/{object:.+}").HeadersRegexp("X-Amz-Rename-Source-Key", ".*?").
			HandlerFunc(api.RenameObjectHandler)
		// PutObjectACL
		bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(api.PutObjectAclHandler).
			Queries("acl", "")
		// GetObjectAcl
		bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(api.GetObjectAclHandler).
			Queries("acl", "")

		// AppendObject
		bucket.Methods("POST").Path("/{object:.+}").HandlerFunc(api.AppendObjectHandler).Queries("append", "")
		// PutObject
		bucket.Methods("PUT").Path("/{object:.+}").HandlerFunc(api.PutObjectHandler)
		// PostObject
		bucket.Methods("POST").HeadersRegexp("Content-Type", "multipart/form-data*").
			HandlerFunc(api.PostObjectHandler)
		// GetObject
		bucket.Methods("GET").Path("/{object:.+}").HandlerFunc(api.GetObjectHandler)
		// DeleteObject
		bucket.Methods("DELETE").Path("/{object:.+}").HandlerFunc(api.DeleteObjectHandler)
		// PutObjectMeta
		bucket.Methods("PUT").Path("/{object:.+}").Queries("meta", "").HandlerFunc(api.PutObjectMeta)

		/// Bucket operations

		// GetBucketLocation
		bucket.Methods("GET").HandlerFunc(api.GetBucketLocationHandler).Queries("location", "")
		// ListMultipartUploads
		bucket.Methods("GET").HandlerFunc(api.ListMultipartUploadsHandler).Queries("uploads", "")
		// Get bucket versioning status
		bucket.Methods("GET").HandlerFunc(api.GetBucketVersioningHandler).Queries("versioning", "")
		// List versioned objects in a bucket
		bucket.Methods("GET").HandlerFunc(api.ListVersionedObjectsHandler).Queries("versions", "")
		// PutBucketACL
		bucket.Methods("PUT").HandlerFunc(api.PutBucketAclHandler).Queries("acl", "")
		// GetBucketACL
		bucket.Methods("GET").HandlerFunc(api.GetBucketAclHandler).Queries("acl", "")
		// PutBucketVersioning
		bucket.Methods("PUT").HandlerFunc(api.PutBucketVersioningHandler).Queries("versioning", "")
		// PutBucketCORS
		bucket.Methods("PUT").HandlerFunc(api.PutBucketCorsHandler).Queries("cors", "")
		// GetBucketCORS
		bucket.Methods("GET").HandlerFunc(api.GetBucketCorsHandler).Queries("cors", "")
		// DeleteBucketCORS
		bucket.Methods("DELETE").HandlerFunc(api.DeleteBucketCorsHandler).Queries("cors", "")
		// PutLifeCycleConfig
		bucket.Methods("PUT").HandlerFunc(api.PutBucketLifeCycleHandler).Queries("lifecycle", "")
		// GetLifeCycleConfig
		bucket.Methods("GET").HandlerFunc(api.GetBucketLifeCycleHandler).Queries("lifecycle", "")
		// DelLifeCycleConfig
		bucket.Methods("DELETE").HandlerFunc(api.DelBucketLifeCycleHandler).Queries("lifecycle", "")
		// PutBucketPolicy
		bucket.Methods("PUT").HandlerFunc(api.PutBucketPolicyHandler).Queries("policy", "")
		// GetBucketPolicy
		bucket.Methods("GET").HandlerFunc(api.GetBucketPolicyHandler).Queries("policy", "")
		// DeleteBucketPolicy
		bucket.Methods("DELETE").HandlerFunc(api.DeleteBucketPolicyHandler).Queries("policy", "")
		// PutBucketWebsite
		bucket.Methods("PUT").HandlerFunc(api.PutBucketWebsiteHandler).Queries("website", "")
		// GetBucketWebsite
		bucket.Methods("GET").HandlerFunc(api.GetBucketWebsiteHandler).Queries("website", "")
		// DeleteBucketWebsite
		bucket.Methods("DELETE").HandlerFunc(api.DeleteBucketWebsiteHandler).Queries("website", "")

		// HeadBucket
		bucket.Methods("HEAD").HandlerFunc(api.HeadBucketHandler)
		// DeleteMultipleObjects
		bucket.Methods("POST").HandlerFunc(api.DeleteMultipleObjectsHandler)
		// DeleteBucket
		bucket.Methods("DELETE").HandlerFunc(api.DeleteBucketHandler)
		// PutBucket
		bucket.Methods("PUT").HandlerFunc(api.PutBucketHandler)
		// ListObjects
		bucket.Methods("GET").HandlerFunc(api.ListObjectsHandler)
	}
	/// Root operation

	// ListBuckets
	apiRouter.Methods("GET").HandlerFunc(api.ListBucketsHandler)
}
