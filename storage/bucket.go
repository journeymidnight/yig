package storage

import (
	"net/url"
	"strings"
	"time"

	"github.com/journeymidnight/yig/api"
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/policy"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/meta/util"
	"github.com/journeymidnight/yig/redis"
)

func (yig *YigStorage) MakeBucket(bucketName string, acl datatype.Acl,
	credential common.Credential) error {
	// Input validation.
	if err := api.CheckValidBucketName(bucketName); err != nil {
		return err
	}

	now := time.Now().UTC()
	bucket := meta.Bucket{
		Name:       bucketName,
		CreateTime: now,
		OwnerId:    credential.UserId,
		ACL:        acl,
		Versioning: meta.VersionDisabled, // it's the default
	}
	processed, err := yig.MetaStorage.Client.CheckAndPutBucket(bucket)
	if err != nil {
		yig.Logger.Error("Error making CheckAndPut:", err)
		return err
	}
	if !processed { // bucket already exists, return accurate message
		bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
		if err != nil {
			yig.Logger.Info("Error get bucket:", bucketName, "with error:", err)
			return ErrBucketAlreadyExists
		}
		if bucket.OwnerId == credential.UserId {
			return ErrBucketAlreadyOwnedByYou
		} else {
			return ErrBucketAlreadyExists
		}
	}
	err = yig.MetaStorage.AddBucketForUser(bucketName, credential.UserId)
	if err != nil { // roll back bucket table, i.e. remove inserted bucket
		yig.Logger.Error("Error AddBucketForUser:", err)
		err = yig.MetaStorage.Client.DeleteBucket(bucket)
		if err != nil {
			yig.Logger.Error("Error deleting:", bucketName, "error:", err,
				"leaving junk bucket unremoved")
			return err
		}
	}
	yig.MetaStorage.Cache.Remove(redis.UserTable, credential.UserId)
	return err
}

func (yig *YigStorage) SetBucketAcl(bucketName string, policy datatype.AccessControlPolicy, acl datatype.Acl,
	credential common.Credential) error {

	if acl.CannedAcl == "" {
		newCannedAcl, err := datatype.GetCannedAclFromPolicy(policy)
		if err != nil {
			return err
		}
		acl = newCannedAcl
	}

	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.ACL = acl
	err = yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
	}
	return nil
}

func (yig *YigStorage) SetBucketLifecycle(bucketName string, lc datatype.Lifecycle,
	credential common.Credential) error {
	helper.Logger.Info("enter SetBucketLifecycle")
	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.Lifecycle = lc
	err = yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
	}

	err = yig.MetaStorage.PutBucketToLifeCycle(*bucket)
	if err != nil {
		yig.Logger.Error("Error Put bucket to lifecycle table:", err)
		return err
	}
	return nil
}

func (yig *YigStorage) GetBucketLifecycle(bucketName string, credential common.Credential) (lc datatype.Lifecycle,
	err error) {
	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		return lc, err
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

func (yig *YigStorage) DelBucketLifecycle(bucketName string, credential common.Credential) error {
	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.Lifecycle = datatype.Lifecycle{}
	err = yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
	}
	err = yig.MetaStorage.RemoveBucketFromLifeCycle(*bucket)
	if err != nil {
		yig.Logger.Error("Remove bucket From lifecycle table error:", err)
		return err
	}
	return nil
}

func (yig *YigStorage) SetBucketCors(bucketName string, cors datatype.Cors,
	credential common.Credential) error {

	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.CORS = cors
	err = yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
	}
	return nil
}

func (yig *YigStorage) DeleteBucketCors(bucketName string, credential common.Credential) error {
	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.CORS = datatype.Cors{}
	err = yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
	}
	return nil
}

func (yig *YigStorage) GetBucketCors(bucketName string,
	credential common.Credential) (cors datatype.Cors, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		return cors, err
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

func (yig *YigStorage) SetBucketVersioning(bucketName string, versioning datatype.Versioning,
	credential common.Credential) error {

	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.Versioning = versioning.Status
	err = yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
	}
	return nil
}

func (yig *YigStorage) GetBucketVersioning(bucketName string, credential common.Credential) (
	versioning datatype.Versioning, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
	if err != nil {
		return versioning, err
	}
	versioning.Status = helper.Ternary(bucket.Versioning == meta.VersionDisabled,
		"", bucket.Versioning).(string)
	return
}

func (yig *YigStorage) GetBucketAcl(bucketName string, credential common.Credential) (
	policy datatype.AccessControlPolicyResponse, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
	if err != nil {
		return policy, err
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

func (yig *YigStorage) GetBucketInfo(bucketName string,
	credential common.Credential) (bucket *meta.Bucket, err error) {

	bucket, err = yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		return
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

func (yig *YigStorage) GetBucketInfoByCtx(ctx *api.RequestContext,
	credential common.Credential) (bucket *meta.Bucket, err error) {

	bucket = ctx.BucketInfo
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
	p := string(data)
	// If policy is empty then delete the bucket policy.
	if p == "" {
		bucket.Policy = policy.Policy{}
	} else {
		bucket.Policy = bucketPolicy
	}

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

	policyBuf, err := bucket.Policy.MarshalJSON()
	if err != nil {
		return
	}
	p, err := policy.ParseConfig(strings.NewReader(string(policyBuf)), bucketName)
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
	bucket.Policy = policy.Policy{}
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
	err := yig.MetaStorage.Client.PutBucket(*bucket)
	if err != nil {
		return err
	}
	yig.MetaStorage.Cache.Remove(redis.BucketTable, bucket.Name)
	return nil
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

func (yig *YigStorage) DeleteBucket(bucketName string, credential common.Credential) (err error) {
	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
		// TODO validate bucket policy
	}

	// Check if bucket is empty
	objs, _, _, _, _, err := yig.MetaStorage.Client.ListObjects(bucketName, "", "", "", "", false, 1)
	if err != nil {
		return err
	}
	if len(objs) != 0 {
		return ErrBucketNotEmpty
	}
	// Check if object part is empty
	objparts, _, _, _, _, err := yig.MetaStorage.Client.ListMultipartUploads(bucketName, "", "", "", "", "", 1)
	if err != nil {
		return err
	}
	if len(objparts) != 0 {
		return ErrBucketNotEmpty
	}
	err = yig.MetaStorage.Client.DeleteBucket(*bucket)
	if err != nil {
		return err
	}

	err = yig.MetaStorage.RemoveBucketForUser(bucketName, credential.UserId)
	if err != nil { // roll back bucket table, i.e. re-add removed bucket entry
		err = yig.MetaStorage.Client.AddBucketForUser(bucketName, credential.UserId)
		if err != nil {
			return err
		}
	}

	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.UserTable, credential.UserId)
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
	}

	if bucket.Lifecycle.Rule != nil {
		err = yig.MetaStorage.RemoveBucketFromLifeCycle(*bucket)
		if err != nil {
			yig.Logger.Warn("Remove bucket from lifeCycle error:", err)
		}
	}

	return nil
}

func (yig *YigStorage) ListObjectsInternal(bucketName string,
	request datatype.ListObjectsRequest) (retObjects []*meta.Object, prefixes []string, truncated bool,
	nextMarker, nextVerIdMarker string, err error) {

	var marker string
	var verIdMarker string
	if request.Versioned {
		marker = request.KeyMarker
		verIdMarker = request.VersionIdMarker
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
	return yig.MetaStorage.Client.ListObjects(bucketName, marker, verIdMarker, request.Prefix, request.Delimiter, request.Versioned, request.MaxKeys)
}

func (yig *YigStorage) ListObjects(credential common.Credential, bucketName string,
	request datatype.ListObjectsRequest) (result meta.ListObjectsInfo, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	helper.Logger.Info("GetBucket", bucket)
	if err != nil {
		return
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

	retObjects, prefixes, truncated, nextMarker, _, err := yig.ListObjectsInternal(bucketName, request)
	if truncated && len(nextMarker) != 0 {
		result.NextMarker = nextMarker
	}
	if request.Version == 2 {
		result.NextMarker = util.Encrypt(result.NextMarker)
	}
	objects := make([]datatype.Object, 0, len(retObjects))
	for _, obj := range retObjects {
		helper.Logger.Info("result:", obj.Name)
		object := datatype.Object{
			LastModified: obj.LastModifiedTime.UTC().Format(meta.CREATE_TIME_LAYOUT),
			ETag:         "\"" + obj.Etag + "\"",
			Size:         obj.Size,
			StorageClass: "STANDARD",
		}
		if request.EncodingType != "" { // only support "url" encoding for now
			object.Key = url.QueryEscape(obj.Name)
		} else {
			object.Key = obj.Name
		}

		if request.FetchOwner {
			var owner common.Credential
			owner, err = iam.GetCredentialByUserId(obj.OwnerId)
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
	result.Prefixes = prefixes
	result.IsTruncated = truncated

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
func (yig *YigStorage) ListVersionedObjects(credential common.Credential, bucketName string,
	request datatype.ListObjectsRequest) (result meta.VersionedListObjectsInfo, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		return
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

	retObjects, prefixes, truncated, nextMarker, nextVerIdMarker, err := yig.ListObjectsInternal(bucketName, request)
	if truncated && len(nextMarker) != 0 {
		result.NextKeyMarker = nextMarker
		result.NextVersionIdMarker = nextVerIdMarker
	}

	objects := make([]datatype.VersionedObject, 0, len(retObjects))
	for _, o := range retObjects {
		// TODO: IsLatest
		object := datatype.VersionedObject{
			LastModified: o.LastModifiedTime.UTC().Format(meta.CREATE_TIME_LAYOUT),
			ETag:         "\"" + o.Etag + "\"",
			Size:         o.Size,
			StorageClass: "STANDARD",
			Key:          o.Name,
		}
		if request.EncodingType != "" { // only support "url" encoding for now
			object.Key = url.QueryEscape(object.Key)
		}
		object.VersionId = o.GetVersionId()
		if o.DeleteMarker {
			object.XMLName.Local = "DeleteMarker"
		} else {
			object.XMLName.Local = "Version"
		}
		if request.FetchOwner {
			var owner common.Credential
			owner, err = iam.GetCredentialByUserId(o.OwnerId)
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
	result.Prefixes = prefixes
	result.IsTruncated = truncated

	if request.EncodingType != "" { // only support "url" encoding for now
		result.Prefixes = helper.Map(result.Prefixes, func(s string) string {
			return url.QueryEscape(s)
		})
		result.NextKeyMarker = url.QueryEscape(result.NextKeyMarker)
	}

	return
}
