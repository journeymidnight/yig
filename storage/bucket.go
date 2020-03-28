package storage

import (
	"bytes"
	"net/url"
	"time"

	"github.com/journeymidnight/yig/crypto"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/policy"
	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/meta/util"
	"github.com/journeymidnight/yig/redis"
)

const (
	BUCKET_NUMBER_LIMIT = 100
)

func (yig *YigStorage) MakeBucket(reqCtx RequestContext, acl datatype.Acl,
	credential common.Credential) error {
	// Input validation.

	if reqCtx.BucketInfo != nil {
		helper.Logger.Info("Error get bucket:", reqCtx.BucketName, "with error:", ErrBucketAlreadyExists)
		return ErrBucketAlreadyExists
	}

	buckets, err := yig.MetaStorage.GetUserBuckets(credential.UserId, false)
	if err != nil {
		return err
	}
	if len(buckets)+1 > BUCKET_NUMBER_LIMIT {
		return ErrTooManyBuckets
	}
	p, _ := policy.Policy{}.MarshalJSON()
	now := time.Now().UTC()
	bucket := meta.Bucket{
		Name:       reqCtx.BucketName,
		CreateTime: now,
		OwnerId:    credential.UserId,
		ACL:        acl,
		Versioning: datatype.BucketVersioningDisabled, // it's the default
		Policy:     p,
	}
	err = yig.MetaStorage.Client.PutNewBucket(bucket)
	if err != nil {
		helper.Logger.Error("Error Put New Bucket:", err)
		return err
	}

	yig.MetaStorage.Cache.Remove(redis.UserTable, credential.UserId)
	return err
}

func (yig *YigStorage) SetBucketAcl(reqCtx RequestContext, policy datatype.AccessControlPolicy, acl datatype.Acl,
	credential common.Credential) error {

	if acl.CannedAcl == "" {
		newCannedAcl, err := datatype.GetCannedAclFromPolicy(policy)
		if err != nil {
			return err
		}
		acl = newCannedAcl
	}
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.ACL = acl
	err := yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, reqCtx.BucketName)
	}
	return nil
}

func (yig *YigStorage) SetBucketLogging(reqCtx RequestContext, bl datatype.BucketLoggingStatus,
	credential common.Credential) error {
	helper.Logger.Info("enter SetBucketLogging")
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.BucketLogging = bl
	err := yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	yig.MetaStorage.Cache.Remove(redis.BucketTable, reqCtx.BucketName)

	return nil
}

func (yig *YigStorage) GetBucketLogging(reqCtx RequestContext, credential common.Credential) (logging datatype.BucketLoggingStatus,
	err error) {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return logging, ErrNoSuchBucket
	}
	if bucket.OwnerId != credential.UserId {
		err = ErrBucketAccessForbidden
		return
	}
	return bucket.BucketLogging, nil
}

func (yig *YigStorage) SetBucketLifecycle(reqCtx RequestContext, lc datatype.Lifecycle,
	credential common.Credential) error {
	helper.Logger.Info("enter SetBucketLifecycle")
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.Lifecycle = lc
	err := yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, reqCtx.BucketName)
	}

	err = yig.MetaStorage.PutBucketToLifeCycle(*bucket)
	if err != nil {
		helper.Logger.Error("Error Put bucket to lifecycle table:", err)
		return err
	}
	return nil
}

func (yig *YigStorage) GetBucketLifecycle(reqCtx RequestContext, credential common.Credential) (lc datatype.Lifecycle,
	err error) {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return lc, ErrNoSuchBucket
	}
	if bucket.OwnerId != credential.UserId {
		err = ErrBucketAccessForbidden
		return
	}
	if len(bucket.Lifecycle.Rule) == 0 {
		err = ErrNoSuchBucketLc
		return
	}
	return bucket.Lifecycle, nil
}

func (yig *YigStorage) DelBucketLifecycle(reqCtx RequestContext, credential common.Credential) error {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.Lifecycle = datatype.Lifecycle{}
	err := yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, reqCtx.BucketName)
	}
	err = yig.MetaStorage.RemoveBucketFromLifeCycle(*bucket)
	if err != nil {
		helper.Logger.Error("Remove bucket From lifecycle table error:", err)
		return err
	}
	return nil
}

func (yig *YigStorage) SetBucketCors(reqCtx RequestContext, cors datatype.Cors,
	credential common.Credential) error {

	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.CORS = cors
	err := yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, reqCtx.BucketName)
	}
	return nil
}

func (yig *YigStorage) DeleteBucketCors(reqCtx RequestContext, credential common.Credential) error {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.CORS = datatype.Cors{}
	err := yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, reqCtx.BucketName)
	}
	return nil
}

func (yig *YigStorage) GetBucketCors(reqCtx RequestContext,
	credential common.Credential) (cors datatype.Cors, err error) {

	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return cors, ErrNoSuchBucket
	}
	if bucket.OwnerId != credential.UserId {
		err = ErrBucketAccessForbidden
		return
	}
	if len(bucket.CORS.CorsRules) == 0 {
		err = ErrNoSuchBucketCors
		return
	}
	return bucket.CORS, nil
}

func (yig *YigStorage) SetBucketVersioning(reqCtx RequestContext, versioning datatype.Versioning,
	credential common.Credential) error {

	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.Versioning = versioning.Status
	err := yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, reqCtx.BucketName)
	}
	return nil
}

func (yig *YigStorage) GetBucketVersioning(reqCtx RequestContext, credential common.Credential) (
	versioning datatype.Versioning, err error) {

	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return versioning, ErrNoSuchBucket
	}
	if bucket.Versioning == datatype.BucketVersioningDisabled {
		return
	}
	versioning.Status = bucket.Versioning
	return
}

func (yig *YigStorage) GetBucketAcl(reqCtx RequestContext, credential common.Credential) (
	policy datatype.AccessControlPolicyResponse, err error) {

	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return policy, ErrNoSuchBucket
	}
	if bucket.OwnerId != credential.UserId {
		err = ErrBucketAccessForbidden
		return
	}
	owner := datatype.Owner{ID: credential.UserId, DisplayName: credential.DisplayName}
	bucketOwner := datatype.Owner{}
	policy, err = datatype.CreatePolicyFromCanned(owner, bucketOwner, bucket.ACL)
	if err != nil {
		return policy, err
	}

	return
}

// For INTERNAL USE ONLY
func (yig *YigStorage) GetBucket(bucketName string) (*meta.Bucket, error) {
	return yig.MetaStorage.GetBucket(bucketName, true)
}

func (yig *YigStorage) GetBucketInfo(reqCtx RequestContext,
	credential common.Credential) (bucket *meta.Bucket, err error) {

	bucket = reqCtx.BucketInfo
	if bucket == nil {
		return nil, ErrNoSuchBucket
	}

	if !credential.AllowOtherUserAccess {
		if bucket.OwnerId != credential.UserId {
			switch bucket.ACL.CannedAcl {
			case "public-read", "public-read-write", "authenticated-read":
				break
			default:
				err = ErrBucketAccessForbidden
				return
			}
		}
	}

	return
}

func (yig *YigStorage) GetBucketInfoByCtx(reqCtx RequestContext,
	credential common.Credential) (bucket *meta.Bucket, err error) {

	bucket = reqCtx.BucketInfo
	if bucket == nil {
		return nil, ErrNoSuchBucket
	}
	if !credential.AllowOtherUserAccess {
		if bucket.OwnerId != credential.UserId {
			switch bucket.ACL.CannedAcl {
			case "public-read", "public-read-write", "authenticated-read":
				break
			default:
				err = ErrBucketAccessForbidden
				return
			}
		}
	}

	return
}

func (yig *YigStorage) SetBucketPolicy(credential common.Credential, bucketName string, bucketPolicy policy.Policy) (err error) {
	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	data, err := bucketPolicy.MarshalJSON()
	if err != nil {
		return
	}

	bucket.Policy = data

	err = yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
	}
	return nil
}

func (yig *YigStorage) GetBucketPolicy(credential common.Credential, bucketName string) (bucketPolicy policy.Policy, err error) {
	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		return
	}
	if bucket.OwnerId != credential.UserId {
		err = ErrBucketAccessForbidden
		return
	}

	p, err := policy.ParseConfig(bytes.NewReader(bucket.Policy), bucketName)
	if err != nil {
		return bucketPolicy, ErrMalformedPolicy
	}

	bucketPolicy = *p
	return
}

func (yig *YigStorage) DeleteBucketPolicy(credential common.Credential, bucketName string) error {
	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.Policy, _ = policy.Policy{}.MarshalJSON()
	err = yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
	}
	return nil
}

func (yig *YigStorage) SetBucketWebsite(bucket *meta.Bucket, config datatype.WebsiteConfiguration) (err error) {
	bucket.Website = config
	err = yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	yig.MetaStorage.Cache.Remove(redis.BucketTable, bucket.Name)
	return nil
}

func (yig *YigStorage) GetBucketWebsite(bucketName string) (config datatype.WebsiteConfiguration, err error) {
	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		return
	}
	return bucket.Website, nil
}

func (yig *YigStorage) DeleteBucketWebsite(bucket *meta.Bucket) error {
	bucket.Website = datatype.WebsiteConfiguration{}
	err := yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	yig.MetaStorage.Cache.Remove(redis.BucketTable, bucket.Name)
	return nil
}

func (yig *YigStorage) SetBucketEncryption(bucket *meta.Bucket, config datatype.EncryptionConfiguration) (err error) {
	bucket.Encryption = config
	err = yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	yig.MetaStorage.Cache.Remove(redis.BucketTable, bucket.Name)
	return nil
}

func (yig *YigStorage) GetBucketEncryption(bucketName string) (config datatype.EncryptionConfiguration, err error) {
	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		return
	}
	return bucket.Encryption, nil
}

func (yig *YigStorage) DeleteBucketEncryption(bucket *meta.Bucket) error {
	bucket.Encryption = datatype.EncryptionConfiguration{}
	err := yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	yig.MetaStorage.Cache.Remove(redis.BucketTable, bucket.Name)
	return nil
}

func (yig *YigStorage) CheckBucketEncryption(bucket *meta.Bucket) (*datatype.ApplyServerSideEncryptionByDefault, bool) {
	bucketEncryption := bucket.Encryption
	if len(bucketEncryption.Rules) == 0 {
		return nil, false
	}
	configuration := bucketEncryption.Rules[0].ApplyServerSideEncryptionByDefault
	if configuration.SSEAlgorithm == crypto.SSEAlgorithmAES256 {
		return configuration, true
	}
	//if bucketEncryption.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm == crypto.SSEAlgorithmKMS {
	//	return configuration, nil
	//}
	return nil, false
}

func (yig *YigStorage) ListBuckets(credential common.Credential) (buckets []meta.Bucket, err error) {
	bucketNames, err := yig.MetaStorage.GetUserBuckets(credential.UserId, true)
	if err != nil {
		return
	}
	for _, bucketName := range bucketNames {
		bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
		if err != nil {
			return buckets, err
		}
		buckets = append(buckets, *bucket)
	}
	return
}

func (yig *YigStorage) DeleteBucket(reqCtx RequestContext, credential common.Credential) (err error) {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
		// TODO validate bucket policy
	}

	bucketName := reqCtx.BucketName

	isEmpty, err := yig.MetaStorage.Client.IsEmptyBucket(bucketName)
	if err != nil {
		return err
	}
	if !isEmpty {
		return ErrBucketNotEmpty
	}
	err = yig.MetaStorage.Client.DeleteBucket(*bucket)
	if err != nil {
		return err
	}

	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.UserTable, credential.UserId)
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
	}

	if bucket.Lifecycle.Rule != nil {
		err = yig.MetaStorage.RemoveBucketFromLifeCycle(*bucket)
		if err != nil {
			helper.Logger.Warn("Remove bucket from lifeCycle error:", err)
		}
	}

	return nil
}

func (yig *YigStorage) ListObjectsInternal(bucket *meta.Bucket, request datatype.ListObjectsRequest) (
	info meta.ListObjectsInfo, err error) {

	var marker string
	if request.Versioned {
		marker = request.KeyMarker
	} else if request.Version == 2 {
		if request.ContinuationToken != "" {
			marker, err = util.Decrypt(request.ContinuationToken)
			if err != nil {
				err = ErrInvalidContinuationToken
				return
			}
		} else {
			marker = request.StartAfter
		}
	} else { // version 1
		marker = request.Marker
	}
	helper.Logger.Info("Prefix:", request.Prefix, "Marker:", request.Marker, "MaxKeys:",
		request.MaxKeys, "Delimiter:", request.Delimiter, "Version:", request.Version,
		"keyMarker:", request.KeyMarker, "versionIdMarker:", request.VersionIdMarker)
	if bucket.Versioning == datatype.BucketVersioningDisabled {
		return yig.MetaStorage.Client.ListObjects(bucket.Name, marker, request.Prefix, request.Delimiter, request.MaxKeys)
	} else {
		return yig.MetaStorage.Client.ListLatestObjects(bucket.Name, marker, request.Prefix, request.Delimiter, request.MaxKeys)
	}
}

func (yig *YigStorage) ListVersionedObjectsInternal(bucketName string,
	request datatype.ListObjectsRequest) (info meta.VersionedListObjectsInfo, err error) {

	var marker string
	var verIdMarker string
	marker = request.KeyMarker
	verIdMarker = request.VersionIdMarker

	helper.Logger.Info("Prefix:", request.Prefix, "Marker:", request.Marker, "MaxKeys:",
		request.MaxKeys, "Delimiter:", request.Delimiter, "Version:", request.Version,
		"keyMarker:", request.KeyMarker, "versionIdMarker:", request.VersionIdMarker)
	return yig.MetaStorage.Client.ListVersionedObjects(bucketName, marker, verIdMarker, request.Prefix, request.Delimiter, request.MaxKeys)
}

func (yig *YigStorage) ListObjects(reqCtx RequestContext, credential common.Credential,
	request datatype.ListObjectsRequest) (result meta.ListObjectsInfo, err error) {

	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return result, ErrNoSuchBucket
	}

	switch bucket.ACL.CannedAcl {
	case "public-read", "public-read-write":
		break
	case "authenticated-read":
		if credential.UserId == "" {
			err = ErrBucketAccessForbidden
			return
		}
	default:
		if bucket.OwnerId != credential.UserId {
			err = ErrBucketAccessForbidden
			return
		}
	}
	// TODO validate user policy and ACL

	info, err := yig.ListObjectsInternal(bucket, request)
	if info.IsTruncated && len(info.NextMarker) != 0 {
		result.NextMarker = info.NextMarker
	}
	if request.Version == 2 {
		result.NextMarker = util.Encrypt(result.NextMarker)
	}
	objects := make([]datatype.Object, 0, len(info.Objects))
	for _, obj := range info.Objects {
		object := datatype.Object{
			LastModified: obj.LastModified,
			ETag:         "\"" + obj.ETag + "\"",
			Size:         obj.Size,
			StorageClass: obj.StorageClass,
		}
		if request.EncodingType != "" { // only support "url" encoding for now
			object.Key = url.QueryEscape(obj.Key)
		} else {
			object.Key = obj.Key
		}

		if request.FetchOwner {
			var owner common.Credential
			owner, err = iam.GetCredentialByUserId(obj.Owner.ID)
			if err != nil {
				return
			}
			object.Owner = datatype.Owner{
				ID:          owner.UserId,
				DisplayName: owner.DisplayName,
			}
		}
		objects = append(objects, object)
	}
	result.Objects = objects
	result.Prefixes = info.Prefixes
	result.IsTruncated = info.IsTruncated

	if request.EncodingType != "" { // only support "url" encoding for now
		result.Prefixes = helper.Map(result.Prefixes, func(s string) string {
			return url.QueryEscape(s)
		})
		result.NextMarker = url.QueryEscape(result.NextMarker)
	}
	return
}

// TODO: refactor, similar to ListObjects
// or not?
func (yig *YigStorage) ListVersionedObjects(reqCtx RequestContext, credential common.Credential,
	request datatype.ListObjectsRequest) (result meta.VersionedListObjectsInfo, err error) {

	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return result, ErrNoSuchBucket
	}

	switch bucket.ACL.CannedAcl {
	case "public-read", "public-read-write":
		break
	case "authenticated-read":
		if credential.UserId == "" {
			err = ErrBucketAccessForbidden
			return
		}
	default:
		if bucket.OwnerId != credential.UserId {
			err = ErrBucketAccessForbidden
			return
		}
	}

	info, err := yig.ListVersionedObjectsInternal(bucket.Name, request)
	if info.IsTruncated && len(info.NextKeyMarker) != 0 {
		result.NextKeyMarker = info.NextKeyMarker
		result.NextVersionIdMarker = info.NextVersionIdMarker
	}

	objects := make([]datatype.VersionedObject, 0, len(info.Objects))
	for _, o := range info.Objects {
		// TODO: IsLatest
		object := datatype.VersionedObject{
			LastModified: o.LastModified,
			ETag:         "\"" + o.ETag + "\"",
			Size:         o.Size,
			StorageClass: o.StorageClass,
			Key:          o.Key,
		}
		if request.EncodingType != "" { // only support "url" encoding for now
			object.Key = url.QueryEscape(object.Key)
		}
		object.VersionId = o.VersionId
		if o.DeleteMarker {
			object.XMLName.Local = "DeleteMarker"
		} else {
			object.XMLName.Local = "Version"
		}
		if request.FetchOwner {
			var owner common.Credential
			owner, err = iam.GetCredentialByUserId(o.Owner.ID)
			if err != nil {
				return
			}
			object.Owner = datatype.Owner{
				ID:          owner.UserId,
				DisplayName: owner.DisplayName,
			}
		}
		objects = append(objects, object)
	}
	result.Objects = objects
	result.Prefixes = info.Prefixes
	result.IsTruncated = info.IsTruncated

	if request.EncodingType != "" { // only support "url" encoding for now
		result.Prefixes = helper.Map(result.Prefixes, func(s string) string {
			return url.QueryEscape(s)
		})
		result.NextKeyMarker = url.QueryEscape(result.NextKeyMarker)
	}

	return
}
