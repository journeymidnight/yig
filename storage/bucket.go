package storage

import (
	"bytes"
	"encoding/json"
	"net/url"
	"time"

	"github.com/journeymidnight/yig/crypto"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/lifecycle"
	"github.com/journeymidnight/yig/api/datatype/policy"
	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	. "github.com/journeymidnight/yig/meta/common"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
)

const (
	BUCKET_NUMBER_LIMIT = 100
)

func (yig *YigStorage) MakeBucket(reqCtx RequestContext, acl datatype.Acl,
	credential common.Credential) error {
	// Input validation.

	if reqCtx.BucketInfo != nil {
		reqCtx.Logger.Info("Error get bucket:", reqCtx.BucketName, "with error:", ErrBucketAlreadyExists)
		return ErrBucketAlreadyExists
	}

	buckets, err := yig.MetaStorage.GetUserBuckets(credential.ExternRootId, false)
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
		OwnerId:    credential.ExternRootId,
		ACL:        acl,
		Versioning: datatype.BucketVersioningDisabled, // it's the default
		Policy:     p,
	}
	err = yig.MetaStorage.Client.PutNewBucket(bucket)
	if err != nil {
		reqCtx.Logger.Fatal("Error Put New Bucket:", err)
		return err
	}

	yig.MetaStorage.Cache.Remove(redis.UserTable, credential.ExternRootId)
	return err
}

func DeepCopyAcl(src, dst *datatype.Acl) {
	dst.CannedAcl = src.CannedAcl
	dst.Policy.DisplayName = src.Policy.DisplayName
	dst.Policy.ID = src.Policy.ID
	dst.Policy.XMLName = src.Policy.XMLName
	dst.Policy.Xmlns = src.Policy.Xmlns
	dst.Policy.AccessControlList = dst.Policy.AccessControlList[0:0]
	for _, grant := range src.Policy.AccessControlList {
		dst.Policy.AccessControlList = append(dst.Policy.AccessControlList, grant)
	}
}

func DeepCopyAclPolicy(src *datatype.AccessControlPolicy, dst *datatype.AccessControlPolicyResponse) error {
	b, err := json.Marshal(src)
	if err != nil {
		return err
	}
	json.Unmarshal(b, dst)
	return nil
}

func (yig *YigStorage) SetBucketAcl(reqCtx RequestContext, acl datatype.Acl,
	credential common.Credential) error {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}

	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write":
					break
				default:
					return ErrBucketAccessForbidden
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					return ErrBucketAccessForbidden
				}
			}
		}
	}
	DeepCopyAcl(&acl, &bucket.ACL)
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
	reqCtx.Logger.Info("enter SetBucketLogging")
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}
	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write":
					break
				default:
					return ErrBucketAccessForbidden
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					return ErrBucketAccessForbidden
				}
			}
		}
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
	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write", "public-read":
					break
				case "authenticated-read":
					if credential.ExternUserId == "" {
						err = ErrBucketAccessForbidden
						return
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			}
		}
	}
	return bucket.BucketLogging, nil
}

func (yig *YigStorage) SetBucketLifecycle(reqCtx RequestContext, lc lifecycle.Lifecycle,
	credential common.Credential) error {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}
	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write":
					break
				default:
					return ErrBucketAccessForbidden
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					return ErrBucketAccessForbidden
				}
			}
		}
	}
	bucket.Lifecycle = lc

	getLC, err := yig.MetaStorage.GetBucketLifeCycle(*bucket)
	if err != nil {
		return err
	}

	// check the lifecycle table for bucket info
	// if not exist, add record to lc table
	if getLC == nil {
		err = yig.MetaStorage.PutBucketToLifeCycle(*bucket)
		if err != nil {
			return err
		}
	} else {
		err := yig.MetaStorage.Client.PutBucket(*bucket)
		if err != nil {
			return err
		}
	}

	yig.MetaStorage.Cache.Remove(redis.BucketTable, reqCtx.BucketName)
	return nil
}

func (yig *YigStorage) GetBucketLifecycle(reqCtx RequestContext, credential common.Credential) (lc lifecycle.Lifecycle,
	err error) {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return lc, ErrNoSuchBucket
	}
	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write", "public-read":
					break
				case "authenticated-read":
					if credential.ExternUserId == "" {
						err = ErrBucketAccessForbidden
						return
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			}
		}
	}
	return bucket.Lifecycle, nil
}

func (yig *YigStorage) DelBucketLifecycle(reqCtx RequestContext, credential common.Credential) error {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}
	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write":
					break
				default:
					return ErrBucketAccessForbidden
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					return ErrBucketAccessForbidden
				}
			}
		}
	}
	bucket.Lifecycle = lifecycle.Lifecycle{}

	err := yig.MetaStorage.RemoveBucketFromLifeCycle(*bucket)
	if err != nil {
		reqCtx.Logger.Error("Remove bucket From lifecycle table error:", err)
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
	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write":
					break
				default:
					return ErrBucketAccessForbidden
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					return ErrBucketAccessForbidden
				}
			}
		}
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
	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write":
					break
				default:
					return ErrBucketAccessForbidden
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					return ErrBucketAccessForbidden
				}
			}
		}
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
	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write", "public-read":
					break
				case "authenticated-read":
					if credential.ExternUserId == "" {
						err = ErrBucketAccessForbidden
						return
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			}
		}
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
	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write":
					break
				default:
					return ErrBucketAccessForbidden
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					return ErrBucketAccessForbidden
				}
			}
		}
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

	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write", "public-read":
					break
				case "authenticated-read":
					if credential.ExternUserId == "" {
						err = ErrBucketAccessForbidden
						return
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			}
		}
	}

	owner := datatype.Owner{ID: bucket.OwnerId, DisplayName: bucket.OwnerId}
	bucketOwner := datatype.Owner{}
	if bucket.ACL.CannedAcl != "" {
		policy, err = datatype.CreatePolicyFromCanned(owner, bucketOwner, bucket.ACL)
	} else {
		DeepCopyAclPolicy(&bucket.ACL.Policy, &policy)
	}

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
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write", "public-read", "authenticated-read":
					break
				default:
					err = ErrBucketAccessForbidden
					return
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_READ, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
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
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write", "public-read", "authenticated-read":
					break
				default:
					err = ErrBucketAccessForbidden
					return
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_READ, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
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

	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write":
					break
				default:
					err = ErrBucketAccessForbidden
					return
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			}
		}
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

	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write", "public-read", "authenticated-read":
					break
				default:
					err = ErrBucketAccessForbidden
					return
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_READ_ACP, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			}
		}
	}

	p, err := policy.ParseConfig(bytes.NewReader(bucket.Policy), bucketName)
	if err != nil {
		return bucketPolicy, ErrMalformedPolicy
	}

	bucketPolicy = *p
	return
}

func (yig *YigStorage) DeleteBucketPolicy(credential common.Credential, bucketName string) (err error) {
	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
	if err != nil {
		return err
	}

	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write":
					break
				default:
					err = ErrBucketAccessForbidden
					return
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_WRITE_ACP, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			}
		}
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
	if bucket == nil {
		return nil, false
	}
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
	bucketNames, err := yig.MetaStorage.GetUserBuckets(credential.ExternRootId, true)
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

	if credential.AllowOtherUserAccess != true {
		return ErrBucketAccessForbidden
	}

	bucketName := reqCtx.BucketName

	isEmpty, err := yig.MetaStorage.Client.IsEmptyBucket(reqCtx.BucketInfo)
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
		yig.MetaStorage.Cache.Remove(redis.UserTable, credential.ExternRootId)
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
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
			marker, err = Decrypt(request.ContinuationToken)
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

	helper.Logger.Info("Prefix:", request.Prefix, "Marker:", request.KeyMarker, "MaxKeys:",
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

	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read", "public-read-write":
					break
				case "authenticated-read":
					if credential.ExternUserId == "" {
						err = ErrBucketAccessForbidden
						return
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_READ, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			}
		}
	}

	info, err := yig.ListObjectsInternal(bucket, request)
	if info.IsTruncated && len(info.NextMarker) != 0 {
		result.NextMarker = info.NextMarker
	}
	if request.Version == 2 {
		result.NextMarker = Encrypt(result.NextMarker)
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
				ID:          owner.ExternUserId,
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

	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read", "public-read-write":
					break
				case "authenticated-read":
					if credential.ExternUserId == "" {
						err = ErrBucketAccessForbidden
						return
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			} else {
				switch true {
				case datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_READ, credential.ExternUserId) ||
					datatype.IsPermissionMatchedById(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ, datatype.ACL_GROUP_TYPE_ALL_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_ALL_USERS):
					break
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_READ, datatype.ACL_GROUP_TYPE_LOG_DELIVERY) ||
					datatype.IsPermissionMatchedByGroup(bucket.ACL.Policy, datatype.ACL_PERM_FULL_CONTROL, datatype.ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					err = ErrBucketAccessForbidden
					return
				}
			}
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
			DeleteMarker: o.DeleteMarker,
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
				ID:          owner.ExternUserId,
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
