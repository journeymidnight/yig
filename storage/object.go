package storage

import (
	"crypto/md5"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
	"path"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/backend"
	. "github.com/journeymidnight/yig/context"
	"github.com/journeymidnight/yig/crypto"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	. "github.com/journeymidnight/yig/meta/common"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/signature"
)

var cMap sync.Map
var latestQueryTime [3]time.Time // 0 is for SMALL_FILE_POOLNAME, 1 is for BIG_FILE_POOLNAME, 2 is for GLACIER_FILE_POOLNAME

const (
	CLUSTER_MAX_USED_SPACE_PERCENT = 85
)

func (yig *YigStorage) pickRandomCluster() (cluster backend.Cluster) {
	helper.Logger.Debug("Error picking cluster from table cluster in DB, " +
		"use first cluster in config to write.")
	for _, c := range yig.DataStorage {
		cluster = c
		break
	}
	return
}

func (yig *YigStorage) pickClusterAndPool(bucket string, object string, storageClass StorageClass,
	size int64, isAppend bool) (cluster backend.Cluster, poolName string) {
	var idx int
	if storageClass == ObjectStorageClassGlacier {
		poolName = backend.GLACIER_FILE_POOLNAME
		idx = 2
	} else {
		if isAppend && size >= helper.CONFIG.BigFileThreshold {
			poolName = backend.BIG_FILE_POOLNAME
			idx = 1
		} else if size < 0 { // request.ContentLength is -1 if length is unknown
			poolName = backend.BIG_FILE_POOLNAME
			idx = 1
		} else if size < helper.CONFIG.BigFileThreshold {
			poolName = backend.SMALL_FILE_POOLNAME
			idx = 0
		} else {
			poolName = backend.BIG_FILE_POOLNAME
			idx = 1
		}
	}

	if v, ok := cMap.Load(poolName); ok {
		return v.(backend.Cluster), poolName
	}

	// TODO: Add Ticker to change Map
	var needCheck bool
	queryTime := latestQueryTime[idx]
	if time.Since(queryTime).Hours() > 24 { // check used space every 24 hours
		latestQueryTime[idx] = time.Now()
		needCheck = true
	}
	var totalWeight int
	clusterWeights := make(map[string]int, len(yig.DataStorage))
	metaClusters, err := yig.MetaStorage.GetClusters()
	if err != nil {
		cluster = yig.pickRandomCluster()
		return
	}
	for _, cluster := range metaClusters {
		if cluster.Weight == 0 {
			continue
		}
		if cluster.Pool != poolName {
			continue
		}
		if needCheck {
			usage, err := yig.DataStorage[cluster.Fsid].GetUsage()
			if err != nil {
				helper.Logger.Warn("Error getting used space: ", err,
					"fsid: ", cluster.Fsid)
				continue
			}
			if usage.UsedSpacePercent > CLUSTER_MAX_USED_SPACE_PERCENT {
				helper.Logger.Warn("Cluster used space exceed ",
					CLUSTER_MAX_USED_SPACE_PERCENT, cluster.Fsid)
				continue
			}
		}
		totalWeight += cluster.Weight
		clusterWeights[cluster.Fsid] = cluster.Weight
	}
	if len(clusterWeights) == 0 || totalWeight == 0 {
		cluster = yig.pickRandomCluster()
		return
	}
	N := rand.Intn(totalWeight)
	n := 0
	for fsid, weight := range clusterWeights {
		n += weight
		if n > N {
			cluster = yig.DataStorage[fsid]
			break
		}
	}
	cMap.Store(poolName, cluster)
	return
}

func (yig *YigStorage) GetClusterByFsName(fsName string) (cluster backend.Cluster, err error) {
	if c, ok := yig.DataStorage[fsName]; ok {
		cluster = c
	} else {
		err = errors.New("Cannot find specified ceph cluster: " + fsName)
	}
	return
}

/*this pool is for download only */
var (
	downloadBufPool sync.Pool
)

func init() {
	downloadBufPool.New = func() interface{} {
		return make([]byte, helper.CONFIG.DownloadBufPoolSize)
	}
}

func generateTransWholeObjectFunc(cluster backend.Cluster,
	object *meta.Object) func(io.Writer) error {

	getWholeObject := func(w io.Writer) error {
		reader, err := cluster.GetReader(object.Pool, object.ObjectId, 0, uint64(object.Size))
		if err != nil {
			return nil
		}
		defer reader.Close()

		buf := downloadBufPool.Get().([]byte)
		_, err = io.CopyBuffer(w, reader, buf)
		downloadBufPool.Put(buf)
		return err
	}
	return getWholeObject
}

func generateTransPartObjectFunc(cephCluster backend.Cluster, object *meta.Object, part *meta.Part, offset, length int64) func(io.Writer) error {
	getNormalObject := func(w io.Writer) error {
		var oid string
		/* the transfered part could be Part or Object */
		if part != nil {
			oid = part.ObjectId
		} else {
			oid = object.ObjectId
		}
		reader, err := cephCluster.GetReader(object.Pool, oid, offset, uint64(length))
		if err != nil {
			return nil
		}
		defer reader.Close()
		buf := downloadBufPool.Get().([]byte)
		_, err = io.CopyBuffer(w, reader, buf)
		downloadBufPool.Put(buf)
		return err
	}
	return getNormalObject
}

// Works together with `wrapAlignedEncryptionReader`, see comments there.
func getAlignedReader(cluster backend.Cluster, poolName, objectName string, objectType meta.ObjectType,
	startOffset int64, length uint64) (reader io.ReadCloser, err error) {

	alignedOffset := startOffset / AES_BLOCK_SIZE * AES_BLOCK_SIZE
	length += uint64(startOffset - alignedOffset)
	return cluster.GetReader(poolName, objectName, alignedOffset, length)
}

func (yig *YigStorage) GetObject(object *meta.Object, startOffset int64,
	length int64, writer io.Writer, sseRequest SseRequest) (err error) {
	var encryptionKey []byte
	if object.SseType == crypto.S3.String() {
		if yig.KMS == nil {
			return ErrKMSNotConfigured
		}
		key, err := yig.KMS.UnsealKey(yig.KMS.GetKeyID(), object.EncryptionKey,
			crypto.Context{object.BucketName: path.Join(object.BucketName, object.Name)})
		if err != nil {
			return err
		}
		encryptionKey = key[:]
	} else { // SSE-C
		if len(sseRequest.CopySourceSseCustomerKey) != 0 {
			encryptionKey = sseRequest.CopySourceSseCustomerKey
		} else {
			encryptionKey = sseRequest.SseCustomerKey
		}
	}

	// throttle write speed as needed
	throttleWriter := yig.MetaStorage.QosMeta.NewThrottleWriter(object.BucketName, writer)
	defer throttleWriter.Close()
	writer = throttleWriter

	if len(object.Parts) == 0 { // this object has only one part
		cephCluster, ok := yig.DataStorage[object.Location]
		if !ok {
			return errors.New("Cannot find specified ceph cluster: " + object.Location)
		}

		transWholeObjectWriter := generateTransWholeObjectFunc(cephCluster, object)

		if object.SseType == "" { // unencrypted object
			transPartObjectWriter := generateTransPartObjectFunc(cephCluster, object,
				nil, startOffset, length)

			return yig.DataCache.WriteFromCache(object, startOffset, length, writer,
				transPartObjectWriter, transWholeObjectWriter)
		}

		// encrypted object
		normalAligenedGet := func() (io.ReadCloser, error) {
			return getAlignedReader(cephCluster, object.Pool, object.ObjectId, object.Type,
				startOffset, uint64(length))
		}
		reader, err := yig.DataCache.GetAlignedReader(object, startOffset, length,
			normalAligenedGet, transWholeObjectWriter)
		if err != nil {
			return err
		}
		defer reader.Close()

		decryptedReader, err := wrapAlignedEncryptionReader(reader, startOffset,
			encryptionKey, object.InitializationVector)
		if err != nil {
			return err
		}
		buffer := downloadBufPool.Get().([]byte)
		_, err = io.CopyBuffer(writer, decryptedReader, buffer)
		downloadBufPool.Put(buffer)
		return err
	}

	// multipart uploaded object
	var low int = object.PartsIndex.SearchLowerBound(startOffset)
	if low == -1 {
		low = 1
	} else {
		//parts number starts from 1, so plus 1 here
		low += 1
	}

	for i := low; i <= len(object.Parts); i++ {
		p := object.Parts[i]
		//for high
		if p.Offset > startOffset+length {
			return
		}
		//for low
		{
			var readOffset, readLength int64
			if startOffset <= p.Offset {
				readOffset = 0
			} else {
				readOffset = startOffset - p.Offset
			}
			if p.Offset+p.Size <= startOffset+length {
				readLength = p.Size - readOffset
			} else {
				readLength = startOffset + length - (p.Offset + readOffset)
			}
			cluster, ok := yig.DataStorage[object.Location]
			if !ok {
				return errors.New("Cannot find specified ceph cluster: " +
					object.Location)
			}
			if object.SseType == "" { // unencrypted object

				transPartFunc := generateTransPartObjectFunc(cluster, object, p, readOffset, readLength)
				err := transPartFunc(writer)
				if err != nil {
					return nil
				}
				continue
			}

			// encrypted object
			err = copyEncryptedPart(object.Pool, p, cluster, readOffset, readLength, encryptionKey, writer)
			if err != nil {
				helper.Logger.Info("Multipart uploaded object write error:", err)
			}
		}
	}
	return
}

func copyEncryptedPart(pool string, part *meta.Part, cluster backend.Cluster,
	readOffset int64, length int64,
	encryptionKey []byte, targetWriter io.Writer) (err error) {

	//pass meta.ObjectTypeMultipart here is no meanling, because pool here must be tiger
	reader, err := getAlignedReader(cluster, pool, part.ObjectId, meta.ObjectTypeMultipart,
		readOffset, uint64(length))
	if err != nil {
		return err
	}
	defer reader.Close()

	decryptedReader, err := wrapAlignedEncryptionReader(reader, readOffset,
		encryptionKey, part.InitializationVector)
	if err != nil {
		return err
	}
	buffer := downloadBufPool.Get().([]byte)
	_, err = io.CopyBuffer(targetWriter, decryptedReader, buffer)
	downloadBufPool.Put(buffer)
	return err
}

func CheckBucketAclForGetObjectInfo(bucket *meta.Bucket, credential common.Credential) (err error) {
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
				case IsPermissionMatchedById(bucket.ACL.Policy, ACL_PERM_READ, credential.ExternUserId) ||
					IsPermissionMatchedById(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_READ, ACL_GROUP_TYPE_ALL_USERS) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_ALL_USERS):
					break
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_READ, ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_READ, ACL_GROUP_TYPE_LOG_DELIVERY) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					return ErrBucketAccessForbidden
				}
			}
		}
	}
	return nil
}

func CheckObjectAclForGetObjectInfo(bucket *meta.Bucket, object *meta.Object, credential common.Credential) (err error) {
	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if object.ACL.CannedAcl != "" {
				switch object.ACL.CannedAcl {
				case "public-read", "public-read-write":
					break
				case "authenticated-read":
					if credential.ExternUserId == "" {
						err = ErrAccessDenied
						return
					}
				default:
					err = ErrAccessDenied
					return
				}
			} else {
				switch true {
				case IsPermissionMatchedById(object.ACL.Policy, ACL_PERM_READ, credential.ExternUserId) ||
					IsPermissionMatchedById(object.ACL.Policy, ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_READ, ACL_GROUP_TYPE_ALL_USERS) ||
					IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_ALL_USERS):
					break
				case IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_READ, ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_READ, ACL_GROUP_TYPE_LOG_DELIVERY) ||
					IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					err = ErrAccessDenied
					return
				}
			}
		}
	}
	return nil
}

func (yig *YigStorage) GetObjectInfo(bucketName string, objectName string,
	version string, credential common.Credential) (object *meta.Object, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		return
	}

	if bucket.Versioning == BucketVersioningDisabled {
		if version != "" {
			return nil, ErrInvalidVersioning
		}
		object, err = yig.MetaStorage.GetObject(bucketName, objectName, meta.NullVersion, true)
	} else {
		if version == "null" {
			version = meta.NullVersion
		}
		object, err = yig.MetaStorage.GetObject(bucketName, objectName, version, true)
	}

	if err != nil {
		return nil, err
	}

	if CheckBucketAclForGetObjectInfo(bucket, credential) != nil && CheckObjectAclForGetObjectInfo(bucket, object, credential) != nil {
		return nil, ErrAccessDenied
	}

	return
}

func (yig *YigStorage) GetObjectInfoByCtx(ctx RequestContext, credential common.Credential) (object *meta.Object, err error) {
	bucket := ctx.BucketInfo
	if bucket == nil {
		return nil, ErrNoSuchBucket
	}

	object = ctx.ObjectInfo
	if object == nil {
		return nil, ErrNoSuchKey
	}

	if CheckBucketAclForGetObjectInfo(bucket, credential) != nil && CheckObjectAclForGetObjectInfo(bucket, object, credential) != nil {
		return nil, ErrAccessDenied
	}

	return
}

func (yig *YigStorage) GetObjectAcl(reqCtx RequestContext, credential common.Credential) (
	policy AccessControlPolicyResponse, err error) {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return policy, ErrNoSuchBucket
	}

	object := reqCtx.ObjectInfo
	if object == nil {
		return policy, ErrNoSuchKey
	}

	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if object.ACL.CannedAcl != "" {
				switch object.ACL.CannedAcl {
				case "public-read", "public-read-write":
					break
				case "authenticated-read":
					if credential.ExternUserId == "" {
						err = ErrAccessDenied
						return
					}
				default:
					err = ErrAccessDenied
					return
				}
			} else {
				switch true {
				case IsPermissionMatchedById(object.ACL.Policy, ACL_PERM_READ_ACP, credential.ExternUserId) ||
					IsPermissionMatchedById(object.ACL.Policy, ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_READ_ACP, ACL_GROUP_TYPE_ALL_USERS) ||
					IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_ALL_USERS):
					break
				case IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_READ_ACP, ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_READ_ACP, ACL_GROUP_TYPE_LOG_DELIVERY) ||
					IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					err = ErrAccessDenied
					return
				}
			}
		}
	}

	owner := Owner{ID: object.OwnerId, DisplayName: object.OwnerId}
	bucketCred, err := iam.GetCredentialByUserId(bucket.OwnerId)
	if err != nil {
		return
	}
	bucketOwner := Owner{ID: bucketCred.ExternUserId, DisplayName: bucketCred.DisplayName}
	if object.ACL.CannedAcl != "" {
		policy, err = CreatePolicyFromCanned(owner, bucketOwner, object.ACL)
	} else {
		DeepCopyAclPolicy(&object.ACL.Policy, &policy)
	}
	if err != nil {
		return
	}

	return
}

func (yig *YigStorage) SetObjectAcl(reqCtx RequestContext, acl Acl,
	credential common.Credential) error {

	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}

	object := reqCtx.ObjectInfo
	if object == nil {
		return ErrNoSuchKey
	}

	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if object.ACL.CannedAcl != "" {
				switch object.ACL.CannedAcl {
				case "public-read-write":
					break
				default:
					return ErrAccessDenied
				}
			} else {
				switch true {
				case IsPermissionMatchedById(object.ACL.Policy, ACL_PERM_WRITE_ACP, credential.ExternUserId) ||
					IsPermissionMatchedById(object.ACL.Policy, ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_WRITE_ACP, ACL_GROUP_TYPE_ALL_USERS) ||
					IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_ALL_USERS):
					break
				case IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_WRITE_ACP, ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_WRITE_ACP, ACL_GROUP_TYPE_LOG_DELIVERY) ||
					IsPermissionMatchedByGroup(object.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_LOG_DELIVERY):
					if helper.StringInSlice(credential.ExternUserId, helper.CONFIG.LogDeliveryGroup) {
						break
					}
				default:
					return ErrAccessDenied
				}
			}
		}
	}

	DeepCopyAcl(&acl, &object.ACL)
	err := yig.MetaStorage.UpdateObjectAcl(object)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.ObjectTable,
			reqCtx.BucketName+":"+reqCtx.ObjectName+":"+reqCtx.ObjectInfo.VersionId)
	}
	return nil
}

// Write path:
//                                           +-----------+
// PUT object/part                           |           |   Ceph
//         +---------+------------+----------+ Encryptor +----->
//                   |            |          |           |
//                   |            |          +-----------+
//                   v            v
//                  SHA256      MD5(ETag)
//
// SHA256 is calculated only for v4 signed authentication
// Encryptor is enabled when user set SSE headers
func (yig *YigStorage) PutObject(reqCtx RequestContext, credential common.Credential,
	size int64, data io.ReadCloser, metadata map[string]string, acl Acl,
	sseRequest SseRequest, storageClass StorageClass) (result PutObjectResult, err error) {
	bucketName, objectName := reqCtx.BucketName, reqCtx.ObjectName
	defer data.Close()
	encryptionKey, cipherKey, err := yig.encryptionKeyFromSseRequest(sseRequest, bucketName, objectName)
	helper.Logger.Debug("get encryptionKey:", encryptionKey, "cipherKey:", cipherKey, "err:", err)
	if err != nil {
		return
	}

	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return result, ErrNoSuchBucket
	}

	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write":
					break
				default:
					// HACK: for put log temporary
					if credential.ExternUserId == "JustForPutLog" {
						credential.ExternUserId = bucket.OwnerId
					} else {
						err = ErrBucketAccessForbidden
						return
					}
				}
			} else {
				switch true {
				case IsPermissionMatchedById(bucket.ACL.Policy, ACL_PERM_WRITE, credential.ExternUserId) ||
					IsPermissionMatchedById(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_ALL_USERS) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_ALL_USERS):
					break
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_LOG_DELIVERY) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_LOG_DELIVERY):
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

	md5Writer := md5.New()

	// Limit the reader to its provided size if specified.
	var limitedDataReader io.Reader
	if size > 0 { // request.ContentLength is -1 if length is unknown
		limitedDataReader = io.LimitReader(data, size)
	} else {
		limitedDataReader = data
	}

	cluster, poolName := yig.pickClusterAndPool(bucketName, objectName, storageClass, size, false)
	if cluster == nil {
		return result, ErrInternalError
	}

	dataReader := io.TeeReader(limitedDataReader, md5Writer)

	var initializationVector []byte
	if len(encryptionKey) != 0 {
		initializationVector, err = newInitializationVector()
		if err != nil {
			return
		}
	}
	// Not support now
	storageReader, err := wrapEncryptionReader(dataReader, encryptionKey, initializationVector)
	if err != nil {
		return
	}

	throttleReader := yig.MetaStorage.QosMeta.NewThrottleReader(bucketName, storageReader)
	defer throttleReader.Close()
	objectId, bytesWritten, err := cluster.Put(poolName, throttleReader)
	if err != nil {
		return
	}
	// Should metadata update failed, add `maybeObjectToRecycle` to `RecycleQueue`,
	// so the object in Ceph could be removed asynchronously
	maybeObjectToRecycle := objectToRecycle{
		location:   cluster.ID(),
		pool:       poolName,
		objectId:   objectId,
		objectType: meta.ObjectTypeNormal,
	}
	if int64(bytesWritten) < size {
		RecycleQueue <- maybeObjectToRecycle
		helper.Logger.Error("Failed to write objects, already written",
			bytesWritten, "total size", size)
		return result, ErrIncompleteBody
	}

	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	helper.Logger.Info("CalculatedMd5:", calculatedMd5, "userMd5:", metadata["md5Sum"])
	if userMd5, ok := metadata["md5Sum"]; ok {
		if userMd5 != "" && userMd5 != calculatedMd5 {
			RecycleQueue <- maybeObjectToRecycle
			return result, ErrBadDigest
		}
	}

	result.Md5 = calculatedMd5

	if signVerifyReader, ok := data.(*signature.SignVerifyReadCloser); ok {
		credential, err = signVerifyReader.Verify()
		if err != nil {
			RecycleQueue <- maybeObjectToRecycle
			return
		}
	}

	// HACK: for put log temporary
	if credential.ExternUserId == "JustForPutLog" {
		credential.ExternUserId = bucket.OwnerId
	}

	// TODO validate bucket policy and fancy ACL
	now := time.Now().UTC()
	object := &meta.Object{
		Name:             objectName,
		BucketName:       bucketName,
		Location:         cluster.ID(),
		Pool:             poolName,
		OwnerId:          credential.ExternRootId,
		Size:             int64(bytesWritten),
		ObjectId:         objectId,
		LastModifiedTime: now,
		Etag:             calculatedMd5,
		ContentType:      metadata["Content-Type"],
		ACL:              acl,
		NullVersion:      helper.Ternary(bucket.Versioning == BucketVersioningEnabled, false, true).(bool),
		DeleteMarker:     false,
		SseType:          sseRequest.Type,
		EncryptionKey: helper.Ternary(sseRequest.Type == crypto.S3.String(),
			cipherKey, []byte("")).([]byte),
		InitializationVector: initializationVector,
		CustomAttributes:     metadata,
		Type:                 meta.ObjectTypeNormal,
		StorageClass:         storageClass,
		CreateTime:           uint64(now.UnixNano()),
	}
	object.VersionId = object.GenVersionId(bucket.Versioning)
	if object.StorageClass == ObjectStorageClassGlacier && bucket.Versioning != BucketVersioningEnabled {
		freezer, err := yig.MetaStorage.GetFreezer(object.BucketName, object.Name, object.VersionId)
		if err == nil {
			err = yig.MetaStorage.DeleteFreezer(freezer)
			if err != nil {
				return result, err
			}
		} else if err != ErrNoSuchKey {
			return result, err
		}
	}

	result.LastModified = object.LastModifiedTime
	result.ObjectSize = object.Size
	if object.VersionId != meta.NullVersion {
		result.VersionId = object.VersionId
	}
	result.DeltaInfo, err = yig.MetaStorage.PutObject(reqCtx, object, nil, true)
	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	} else if reqCtx.ObjectInfo != nil {
		yig.MetaStorage.Cache.Remove(redis.ObjectTable, bucketName+":"+objectName+":"+object.VersionId)
		yig.DataCache.Remove(bucketName + ":" + objectName + ":" + object.VersionId)
		if reqCtx.BucketInfo.Versioning != BucketVersioningEnabled {
			go yig.removeOldObject(reqCtx.ObjectInfo)
		}
	}
	return result, nil
}

func (yig *YigStorage) PutObjectMeta(bucket *meta.Bucket, targetObject *meta.Object, credential common.Credential) (err error) {
	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write":
					break
				default:
					// HACK: for put log temporary
					if credential.ExternUserId == "JustForPutLog" {
						credential.ExternUserId = bucket.OwnerId
					} else {
						err = ErrBucketAccessForbidden
						return
					}
				}
			} else {
				switch true {
				case IsPermissionMatchedById(bucket.ACL.Policy, ACL_PERM_WRITE, credential.ExternUserId) ||
					IsPermissionMatchedById(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_ALL_USERS) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_ALL_USERS):
					break
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_LOG_DELIVERY) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_LOG_DELIVERY):
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

	err = yig.MetaStorage.UpdateObjectAttrs(targetObject)
	if err != nil {
		helper.Logger.Error("Update Object Attrs, sql fails:", err)
		return ErrInternalError
	}

	yig.MetaStorage.Cache.Remove(redis.ObjectTable, targetObject.BucketName+":"+targetObject.Name+":"+targetObject.VersionId)
	yig.DataCache.Remove(targetObject.BucketName + ":" + targetObject.Name + ":" + targetObject.VersionId)

	return nil
}

func (yig *YigStorage) RenameObject(reqCtx RequestContext, targetObject *meta.Object, sourceObject string, credential common.Credential) (result RenameObjectResult, err error) {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return result, ErrNoSuchBucket
	}

	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(bucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if bucket.ACL.CannedAcl != "" {
				switch bucket.ACL.CannedAcl {
				case "public-read-write":
					break
				default:
					// HACK: for put log temporary
					if credential.ExternUserId == "JustForPutLog" {
						credential.ExternUserId = bucket.OwnerId
					} else {
						err = ErrBucketAccessForbidden
						return
					}
				}
			} else {
				switch true {
				case IsPermissionMatchedById(bucket.ACL.Policy, ACL_PERM_WRITE, credential.ExternUserId) ||
					IsPermissionMatchedById(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_ALL_USERS) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_ALL_USERS):
					break
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_LOG_DELIVERY) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_LOG_DELIVERY):
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

	err = yig.MetaStorage.RenameObject(targetObject, sourceObject)
	if err != nil {
		helper.Logger.Error("Update Object Attrs, sql fails:", err)
		return result, ErrInternalError

	}

	result.LastModified = targetObject.LastModifiedTime
	yig.MetaStorage.Cache.Remove(redis.ObjectTable, targetObject.BucketName+":"+targetObject.Name+":"+targetObject.VersionId)
	yig.DataCache.Remove(targetObject.BucketName + ":" + targetObject.Name + ":" + targetObject.VersionId)

	return result, nil
}

func (yig *YigStorage) CopyObject(reqCtx RequestContext, targetObject *meta.Object, sourceObject *meta.Object, source io.Reader, credential common.Credential,
	sseRequest SseRequest, isMetadataOnly, isTranStorageClassOnly bool) (result PutObjectResult, err error) {
	result.DeltaInfo = make(map[StorageClass]int64)
	var oid string
	var maybeObjectToRecycle objectToRecycle
	var encryptionKey []byte
	encryptionKey, cipherKey, err := yig.encryptionKeyFromSseRequest(sseRequest, targetObject.BucketName, targetObject.Name)
	if err != nil {
		return
	}

	targetBucket := reqCtx.BucketInfo
	if targetBucket == nil {
		return result, ErrNoSuchBucket
	}

	if !credential.AllowOtherUserAccess {
		//an CanonicalUser request
		if !(targetBucket.OwnerId == credential.ExternRootId && credential.ExternUserId == credential.ExternRootId) {
			if targetBucket.ACL.CannedAcl != "" {
				switch targetBucket.ACL.CannedAcl {
				case "public-read-write":
					break
				default:
					err = ErrBucketAccessForbidden
					return
				}
			} else {
				switch true {
				case IsPermissionMatchedById(targetBucket.ACL.Policy, ACL_PERM_WRITE, credential.ExternUserId) ||
					IsPermissionMatchedById(targetBucket.ACL.Policy, ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case IsPermissionMatchedByGroup(targetBucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_ALL_USERS) ||
					IsPermissionMatchedByGroup(targetBucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_ALL_USERS):
					break
				case IsPermissionMatchedByGroup(targetBucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					IsPermissionMatchedByGroup(targetBucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case IsPermissionMatchedByGroup(targetBucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_LOG_DELIVERY) ||
					IsPermissionMatchedByGroup(targetBucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_LOG_DELIVERY):
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

	if isMetadataOnly {
		targetObject.LastModifiedTime = sourceObject.LastModifiedTime
		targetObject.VersionId = sourceObject.VersionId

		err = yig.MetaStorage.ReplaceObjectMetas(targetObject)
		if err != nil {
			helper.Logger.Error("Copy Object with same source and target, sql fails:", err)
			return result, ErrInternalError
		}

		yig.MetaStorage.Cache.Remove(redis.ObjectTable, targetObject.BucketName+":"+targetObject.Name+":"+targetObject.VersionId)
		yig.DataCache.Remove(targetObject.BucketName + ":" + targetObject.Name + ":" + targetObject.VersionId)

		if targetObject.StorageClass != sourceObject.StorageClass {
			result.DeltaInfo[sourceObject.StorageClass] -= sourceObject.Size
			result.DeltaInfo[targetObject.StorageClass] += targetObject.Size
		}
		return result, nil
	}

	// Limit the reader to its provided size if specified.
	var limitedDataReader io.Reader
	limitedDataReader = io.LimitReader(source, targetObject.Size)

	cephCluster, poolName := yig.pickClusterAndPool(targetObject.BucketName,
		targetObject.Name, targetObject.StorageClass, targetObject.Size, false)

	if len(targetObject.Parts) != 0 {
		var targetParts = make(map[int]*meta.Part, len(targetObject.Parts))
		//		etaglist := make([]string, len(sourceObject.Parts))
		for i := 1; i <= len(targetObject.Parts); i++ {
			part := targetObject.Parts[i]
			targetParts[i] = part
			result, err = func() (result PutObjectResult, err error) {
				pr, pw := io.Pipe()
				defer pr.Close()
				var total = part.Size
				go func() {
					_, err = io.CopyN(pw, source, total)
					if err != nil {
						return
					}
					pw.Close()
				}()
				md5Writer := md5.New()
				dataReader := io.TeeReader(pr, md5Writer)
				var bytesW uint64
				var storageReader io.Reader
				var initializationVector []byte
				if len(encryptionKey) != 0 {
					initializationVector, err = newInitializationVector()
					if err != nil {
						return
					}
				}
				storageReader, err = wrapEncryptionReader(dataReader, encryptionKey, initializationVector)
				throttleReader := yig.MetaStorage.QosMeta.NewThrottleReader(targetBucket.Name, storageReader)
				defer throttleReader.Close()
				oid, bytesW, err = cephCluster.Put(poolName, throttleReader)
				maybeObjectToRecycle = objectToRecycle{
					location:   cephCluster.ID(),
					pool:       poolName,
					objectId:   oid,
					objectType: meta.ObjectTypeMultipart,
				}
				if bytesW < uint64(part.Size) {
					RecycleQueue <- maybeObjectToRecycle
					helper.Logger.Error("Copy part", i, "error:", bytesW, part.Size)
					return result, ErrIncompleteBody
				}
				if err != nil {
					return result, err
				}
				calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
				//we will only chack part etag,overall etag will be same if each part of etag is same
				if calculatedMd5 != part.Etag {
					err = ErrInternalError
					RecycleQueue <- maybeObjectToRecycle
					return result, err
				}
				part.LastModified = time.Now().UTC().Format(meta.CREATE_TIME_LAYOUT)
				part.ObjectId = oid

				part.InitializationVector = initializationVector
				return result, nil
			}()
			if err != nil {
				return result, err
			}
		}
		targetObject.ObjectId = ""
		targetObject.Parts = targetParts
		result.Md5 = targetObject.Etag
	} else {
		md5Writer := md5.New()

		// Mapping a shorter name for the object
		dataReader := io.TeeReader(limitedDataReader, md5Writer)
		var storageReader io.Reader
		var initializationVector []byte
		if len(encryptionKey) != 0 {
			initializationVector, err = newInitializationVector()
			if err != nil {
				return
			}
		}
		storageReader, err = wrapEncryptionReader(dataReader, encryptionKey, initializationVector)
		if err != nil {
			return
		}
		var bytesWritten uint64
		throttleReader := yig.MetaStorage.QosMeta.NewThrottleReader(targetBucket.Name, storageReader)
		defer throttleReader.Close()
		oid, bytesWritten, err = cephCluster.Put(poolName, throttleReader)
		if err != nil {
			return
		}
		// Should metadata update failed, add `maybeObjectToRecycle` to `RecycleQueue`,
		// so the object in Ceph could be removed asynchronously
		maybeObjectToRecycle = objectToRecycle{
			location:   cephCluster.ID(),
			pool:       poolName,
			objectId:   oid,
			objectType: meta.ObjectTypeNormal,
		}
		if int64(bytesWritten) < targetObject.Size {
			RecycleQueue <- maybeObjectToRecycle
			helper.Logger.Error("Copy ", "error:", bytesWritten, targetObject.Size)
			return result, ErrIncompleteBody
		}

		calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
		if calculatedMd5 != targetObject.Etag {
			RecycleQueue <- maybeObjectToRecycle
			return result, ErrBadDigest
		}
		result.Md5 = calculatedMd5
		targetObject.ObjectId = oid
		targetObject.InitializationVector = initializationVector
	}
	// TODO validate bucket policy and fancy ACL

	targetObject.Location = cephCluster.ID()
	targetObject.Pool = poolName
	targetObject.OwnerId = credential.ExternRootId
	targetObject.NullVersion = helper.Ternary(targetBucket.Versioning == BucketVersioningEnabled, false, true).(bool)
	targetObject.DeleteMarker = false
	targetObject.SseType = sseRequest.Type
	targetObject.EncryptionKey = helper.Ternary(sseRequest.Type == crypto.S3.String(),
		cipherKey, []byte("")).([]byte)
	targetObject.LastModifiedTime = time.Now().UTC()
	targetObject.CreateTime = uint64(targetObject.LastModifiedTime.UnixNano())
	if isTranStorageClassOnly {
		targetObject.VersionId = sourceObject.VersionId
	} else {
		targetObject.VersionId = targetObject.GenVersionId(targetBucket.Versioning)
	}

	result.LastModified = targetObject.LastModifiedTime
	// Non-glacier object to glacier object with same object name and bucket name
	if targetObject.StorageClass == ObjectStorageClassGlacier && targetObject.Name == sourceObject.Name && targetObject.BucketName == sourceObject.BucketName {
		targetObject.LastModifiedTime = sourceObject.LastModifiedTime
		result.LastModified = targetObject.LastModifiedTime
		targetObject.CreateTime = sourceObject.CreateTime
		err = yig.MetaStorage.UpdateGlacierObject(reqCtx, targetObject, sourceObject)
		result.DeltaInfo[sourceObject.StorageClass] -= sourceObject.Size
		result.DeltaInfo[targetObject.StorageClass] += targetObject.Size
	} else {
		result.DeltaInfo, err = yig.MetaStorage.PutObject(reqCtx, targetObject, nil, true)
	}
	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	} else {
		yig.MetaStorage.Cache.Remove(redis.ObjectTable, targetObject.BucketName+":"+targetObject.Name+":"+targetObject.VersionId)
		yig.DataCache.Remove(targetObject.BucketName + ":" + targetObject.Name + ":" + targetObject.VersionId)
		if reqCtx.ObjectInfo != nil && reqCtx.BucketInfo.Versioning != BucketVersioningEnabled {
			go yig.removeOldObject(reqCtx.ObjectInfo)
		}
	}

	return result, nil
}

func (yig *YigStorage) removeByObject(object *meta.Object) (err error) {
	err = yig.MetaStorage.DeleteObject(object)
	if err != nil {
		return
	}
	return nil
}

func (yig *YigStorage) removeOldObject(object *meta.Object) (err error) {
	err = yig.MetaStorage.PutObjectToGarbageCollection(object)
	if err != nil {
		return err
	}

	if object.StorageClass == ObjectStorageClassGlacier {
		freezer, err := yig.GetFreezer(object.BucketName, object.Name, object.VersionId)
		if err == nil {
			if freezer.Name == object.Name {
				err = yig.MetaStorage.DeleteFreezer(freezer)
				if err != nil {
					return err
				}
			}
		} else if err != ErrNoSuchKey {
			return err
		}
	}

	return
}

func (yig *YigStorage) removeObjectVersion(bucketName, objectName, version string) error {
	return nil
}

//TODO: Append Support Encryption
func (yig *YigStorage) AppendObject(reqCtx RequestContext, credential common.Credential,
	offset uint64, size int64, data io.ReadCloser, metadata map[string]string, acl Acl,
	sseRequest SseRequest, storageClass StorageClass, objInfo *meta.Object) (result AppendObjectResult, err error) {
	prepareStart := time.Now()
	bucketName, objectName := reqCtx.BucketName, reqCtx.ObjectName
	defer data.Close()
	encryptionKey, cipherKey, err := yig.encryptionKeyFromSseRequest(sseRequest, bucketName, objectName)
	helper.Logger.Info("get encryptionKey:", encryptionKey, "cipherKey:", cipherKey, "err:", err)
	if err != nil {
		return
	}

	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return result, ErrNoSuchBucket
	}

	if objInfo != nil {
		if objInfo.StorageClass == ObjectStorageClassGlacier {
			freezer, err := yig.GetFreezer(objInfo.BucketName, objInfo.Name, objInfo.VersionId)
			if err == nil {
				if freezer.Name == objInfo.Name {
					err = yig.MetaStorage.DeleteFreezer(freezer)
					if err != nil {
						return result, err
					}
				}
			} else if err != ErrNoSuchKey {
				return result, err
			}
		}
	} else {
		if storageClass == ObjectStorageClassGlacier {
			return result, ErrMethodNotAllowed
		}
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
				case IsPermissionMatchedById(bucket.ACL.Policy, ACL_PERM_WRITE, credential.ExternUserId) ||
					IsPermissionMatchedById(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_ALL_USERS) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_ALL_USERS):
					break
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_LOG_DELIVERY) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_LOG_DELIVERY):
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
	//TODO: Append Support Encryption
	encryptionKey = nil

	md5Writer := md5.New()

	// Limit the reader to its provided size if specified.
	var limitedDataReader io.Reader
	if size > 0 { // request.ContentLength is -1 if length is unknown
		limitedDataReader = io.LimitReader(data, size)
	} else {
		limitedDataReader = data
	}

	var cephCluster backend.Cluster
	var poolName, oid string
	var initializationVector []byte
	var objSize int64
	if objInfo != nil {
		cephCluster = yig.DataStorage[objInfo.Location]
		// Every appendable file must be treated as a big file
		poolName = objInfo.Pool
		oid = objInfo.ObjectId
		initializationVector = objInfo.InitializationVector
		objSize = objInfo.Size
		storageClass = objInfo.StorageClass
		helper.Logger.Info("request append oid:", oid, "iv:", initializationVector, "size:", objSize)
	} else {
		// New appendable object
		cephCluster, poolName = yig.pickClusterAndPool(bucketName, objectName, storageClass, size, true)
		if cephCluster == nil {
			helper.Logger.Warn("PickOneClusterAndPool error")
			return result, ErrInternalError
		}
		if len(encryptionKey) != 0 {
			initializationVector, err = newInitializationVector()
			if err != nil {
				return
			}
		}
		helper.Logger.Info("request first append oid:", oid, "iv:", initializationVector, "size:", objSize)
	}

	dataReader := io.TeeReader(limitedDataReader, md5Writer)

	storageReader, err := wrapEncryptionReader(dataReader, encryptionKey, initializationVector)
	if err != nil {
		return
	}

	throttleReader := yig.MetaStorage.QosMeta.NewThrottleReader(bucketName, storageReader)
	defer throttleReader.Close()
	prepareEnd := time.Now()
	oid, bytesWritten, err := cephCluster.Append(poolName, oid, throttleReader, int64(offset), size)
	if err != nil {
		helper.Logger.Error("cephCluster.Append err:", err, poolName, oid, offset)
		return
	}
	appendEnd := time.Now()
	if int64(bytesWritten) < size {
		return result, ErrIncompleteBody
	}

	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	if userMd5, ok := metadata["md5Sum"]; ok {
		if userMd5 != "" && userMd5 != calculatedMd5 {
			return result, ErrBadDigest
		}
	}

	result.Md5 = calculatedMd5

	if signVerifyReader, ok := data.(*signature.SignVerifyReadCloser); ok {
		credential, err = signVerifyReader.Verify()
		if err != nil {
			return
		}
	}

	// TODO validate bucket policy and fancy ACL
	now := time.Now().UTC()
	object := &meta.Object{
		Name:                 objectName,
		BucketName:           bucketName,
		Location:             cephCluster.ID(),
		Pool:                 poolName,
		OwnerId:              credential.ExternRootId,
		Size:                 objSize + int64(bytesWritten),
		ObjectId:             oid,
		LastModifiedTime:     now,
		Etag:                 calculatedMd5,
		ContentType:          metadata["Content-Type"],
		ACL:                  acl,
		NullVersion:          true,
		DeleteMarker:         false,
		SseType:              sseRequest.Type,
		EncryptionKey:        []byte(""),
		InitializationVector: initializationVector,
		CustomAttributes:     metadata,
		Type:                 meta.ObjectTypeAppendable,
		StorageClass:         storageClass,
		VersionId:            meta.NullVersion,
		CreateTime:           uint64(now.UnixNano()),
		DeltaSize:            int64(bytesWritten),
	}

	result.LastModified = object.LastModifiedTime
	result.NextPosition = object.Size
	md5End := time.Now()
	err = yig.MetaStorage.AppendObject(object, objInfo)
	if err != nil {
		return
	}
	metaEnd := time.Now()
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.ObjectTable, bucketName+":"+objectName+":"+meta.NullVersion)
		yig.DataCache.Remove(bucketName + ":" + objectName + ":" + object.VersionId)
	}
	redisEnd := time.Now()
	helper.Logger.Info("Append info.", "bucket:", bucketName, "objName:", objectName, "oid:", oid,
		"objSize:", object.Size, "bytesWritten:", bytesWritten, "storageClass:", storageClass,
		"prepareCost:", prepareEnd.Sub(prepareStart).Milliseconds(),
		"appendCost:", appendEnd.Sub(prepareEnd).Milliseconds(),
		"md5Cost:", md5End.Sub(appendEnd).Milliseconds(),
		"metaCost:", metaEnd.Sub(md5End).Milliseconds(),
		"redisCost:", redisEnd.Sub(metaEnd).Milliseconds())
	return result, nil
}

// When bucket versioning is Disabled/Enabled/Suspended, and request versionId is set/unset:
//
// |           |        with versionId        |                   without versionId                    |
// |-----------|------------------------------|--------------------------------------------------------|
// | Disabled  | error                        | remove object                                          |
// | Enabled   | remove corresponding version | add a delete marker                                    |
// | Suspended | remove corresponding version | remove null version object(if exists) and add a        |
// |           |                              | null version delete marker                             |
//
// See http://docs.aws.amazon.com/AmazonS3/latest/dev/Versioning.html
func (yig *YigStorage) DeleteObject(reqCtx RequestContext,
	credential common.Credential) (result DeleteObjectResult, err error) {

	bucket, object := reqCtx.BucketInfo, reqCtx.ObjectInfo
	if bucket == nil {
		return result, ErrNoSuchBucket
	}

	bucketName, objectName, reqVersion := reqCtx.BucketName, reqCtx.ObjectName, reqCtx.VersionId

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
				case IsPermissionMatchedById(bucket.ACL.Policy, ACL_PERM_WRITE, credential.ExternUserId) ||
					IsPermissionMatchedById(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_ALL_USERS) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_ALL_USERS):
					break
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_LOG_DELIVERY) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_LOG_DELIVERY):
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

	switch bucket.Versioning {
	case BucketVersioningDisabled:
		if reqVersion != "" && reqVersion != meta.NullVersion {
			return result, ErrNoSuchVersion
		}
		if object == nil {
			return result, nil
		}
		err = yig.MetaStorage.DeleteObject(object)
		if err != nil {
			return
		}
		result.DeltaSize = DeltaSizeInfo{StorageClass: object.StorageClass, Delta: -object.Size}
	case BucketVersioningEnabled:
		if reqVersion != "" {
			if object == nil {
				return result, nil
			}
			err = yig.MetaStorage.DeleteObject(object)
			if err != nil {
				return
			}
			if object.DeleteMarker {
				result.DeleteMarker = true
			}
			result.VersionId = object.VersionId
			result.DeltaSize = DeltaSizeInfo{StorageClass: object.StorageClass, Delta: -object.Size}
		} else {
			// Add delete marker                                    |
			if object == nil {
				object = &meta.Object{
					BucketName: bucketName,
					Name:       objectName,
					OwnerId:    bucket.OwnerId,
				}
			}
			object.DeleteMarker = true
			object.LastModifiedTime = time.Now().UTC()
			object.CreateTime = uint64(object.LastModifiedTime.UnixNano())
			object.VersionId = object.GenVersionId(bucket.Versioning)
			object.Size = int64(len(object.Name))
			err = yig.MetaStorage.AddDeleteMarker(object)
			if err != nil {
				return
			}
			result.VersionId = object.VersionId
			//TODO: develop inherit storage class of bucket？
			result.DeltaSize = DeltaSizeInfo{StorageClass: ObjectStorageClassStandard, Delta: object.Size}
		}
	case BucketVersioningSuspended:
		if reqVersion != "" {
			if object == nil {
				return result, nil
			}
			err = yig.MetaStorage.DeleteObject(object)
			if err != nil {
				return
			}
			if object.DeleteMarker {
				result.DeleteMarker = true
			}
			result.VersionId = object.VersionId
			result.DeltaSize = DeltaSizeInfo{StorageClass: object.StorageClass, Delta: -object.Size}
		} else {
			nullVersionExist := (object != nil)
			if !nullVersionExist {
				now := time.Now().UTC()
				object = &meta.Object{
					BucketName:       bucketName,
					Name:             objectName,
					OwnerId:          bucket.OwnerId,
					DeleteMarker:     true,
					LastModifiedTime: now,
					VersionId:        meta.NullVersion,
					CreateTime:       uint64(now.UnixNano()),
					Size:             int64(len(object.Name)),
				}
				err = yig.MetaStorage.AddDeleteMarker(object)
				if err != nil {
					return
				}
				result.DeltaSize = DeltaSizeInfo{StorageClass: ObjectStorageClassStandard, Delta: object.Size}
			} else {
				err = yig.MetaStorage.DeleteSuspendedObject(object)
				if err != nil {
					return
				}
				result.DeltaSize = DeltaSizeInfo{StorageClass: object.StorageClass, Delta: -object.Size}
			}
		}
	default:
		helper.Logger.Error("Invalid bucket versioning:", bucketName)
		return result, ErrInternalError
	}

	if err == nil {
		if reqVersion != "" {
			yig.MetaStorage.Cache.Remove(redis.ObjectTable, bucketName+":"+objectName+":"+reqVersion)
			yig.DataCache.Remove(bucketName + ":" + objectName + ":" + reqVersion)
		} else if reqCtx.ObjectInfo != nil {
			yig.MetaStorage.Cache.Remove(redis.ObjectTable, bucketName+":"+objectName+":"+reqCtx.ObjectInfo.VersionId)
			yig.DataCache.Remove(bucketName + ":" + objectName + ":" + reqCtx.ObjectInfo.VersionId)
		}
	}
	return result, nil
}

func (yig *YigStorage) DeleteObjects(reqCtx RequestContext, credential common.Credential,
	objects []ObjectIdentifier) (result DeleteObjectsResult, err error) {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return result, ErrNoSuchBucket
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
				case IsPermissionMatchedById(bucket.ACL.Policy, ACL_PERM_WRITE, credential.ExternUserId) ||
					IsPermissionMatchedById(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, credential.ExternUserId):
					break
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_ALL_USERS) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_ALL_USERS):
					break
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_AUTHENTICATED_USERS) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_AUTHENTICATED_USERS):
					if credential.ExternUserId != "" {
						break
					}
				case IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_WRITE, ACL_GROUP_TYPE_LOG_DELIVERY) ||
					IsPermissionMatchedByGroup(bucket.ACL.Policy, ACL_PERM_FULL_CONTROL, ACL_GROUP_TYPE_LOG_DELIVERY):
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

	var tx driver.Tx
	tx, err = yig.MetaStorage.NewTrans()
	if err != nil {
		return result, err
	}
	defer func() {
		if err == nil {
			err = yig.MetaStorage.CommitTrans(tx)
		}
		if err != nil {
			yig.MetaStorage.AbortTrans(tx)
		}
	}()

	getFunc := func(o ObjectIdentifier, tx driver.Tx) (object *meta.Object, err error) {
		// GetObject
		bucketName, objectName, version := bucket.Name, o.ObjectName, o.VersionId
		if version == "null" {
			version = meta.NullVersion
		}
		if bucket.Versioning == BucketVersioningDisabled {
			if version != "" && version != meta.NullVersion {
				return object, ErrInvalidVersioning
			}
			object, err = yig.MetaStorage.GetObject(bucketName, objectName, meta.NullVersion, false)
		} else {
			object, err = yig.MetaStorage.GetObject(bucketName, objectName, version, false)
		}
		return object, err
	}

	deleteFunc := func(object *meta.Object, tx driver.Tx) (result DeleteObjectResult, err error) {
		bucketName, objectName, version := bucket.Name, object.Name, object.VersionId
		switch bucket.Versioning {
		case BucketVersioningDisabled:
			if version != "" && version != meta.NullVersion {
				return result, ErrNoSuchVersion
			}
			if object == nil {
				return result, nil
			}
			err = yig.MetaStorage.DeleteObjectWithTx(object, tx)
			if err != nil {
				return result, err
			}
			result.DeltaSize = DeltaSizeInfo{StorageClass: object.StorageClass, Delta: -object.Size}
		case BucketVersioningEnabled:
			if version != "" {
				if object == nil {
					return result, nil
				}
				err = yig.MetaStorage.DeleteObjectWithTx(object, tx)
				if err != nil {
					return
				}
				if object.DeleteMarker {
					result.DeleteMarker = true
				}
				result.VersionId = object.VersionId
				result.DeltaSize = DeltaSizeInfo{StorageClass: object.StorageClass, Delta: -object.Size}
			} else {
				// Add delete marker                                    |
				if object == nil {
					object = &meta.Object{
						BucketName: bucketName,
						Name:       objectName,
						OwnerId:    bucket.OwnerId,
					}
				}
				object.DeleteMarker = true
				object.LastModifiedTime = time.Now().UTC()
				object.CreateTime = uint64(object.LastModifiedTime.UnixNano())
				object.VersionId = object.GenVersionId(bucket.Versioning)
				object.Size = int64(len(object.Name))
				err = yig.MetaStorage.AddDeleteMarker(object)
				if err != nil {
					return
				}
				result.VersionId = object.VersionId
				//TODO: develop inherit storage class of bucket？
				result.DeltaSize = DeltaSizeInfo{StorageClass: ObjectStorageClassStandard, Delta: object.Size}
			}
		case BucketVersioningSuspended:
			if version != "" {
				if object == nil {
					return result, nil
				}
				err = yig.MetaStorage.DeleteObjectWithTx(object, tx)
				if err != nil {
					return
				}
				if object.DeleteMarker {
					result.DeleteMarker = true
				}
				result.VersionId = object.VersionId
				result.DeltaSize = DeltaSizeInfo{StorageClass: object.StorageClass, Delta: -object.Size}
			} else {
				nullVersionExist := (object != nil)
				if !nullVersionExist {
					now := time.Now().UTC()
					object = &meta.Object{
						BucketName:       bucketName,
						Name:             objectName,
						OwnerId:          bucket.OwnerId,
						DeleteMarker:     true,
						LastModifiedTime: now,
						VersionId:        meta.NullVersion,
						CreateTime:       uint64(now.UnixNano()),
						Size:             int64(len(object.Name)),
					}
					err = yig.MetaStorage.AddDeleteMarker(object)
					if err != nil {
						return
					}
					result.DeltaSize = DeltaSizeInfo{StorageClass: ObjectStorageClassStandard, Delta: object.Size}
				} else {
					err = yig.MetaStorage.DeleteSuspendedObject(object)
					if err != nil {
						return
					}
					result.DeltaSize = DeltaSizeInfo{StorageClass: object.StorageClass, Delta: -object.Size}
				}
			}
		default:
			helper.Logger.Error("Invalid bucket versioning:", bucketName)
			return result, ErrInternalError
		}
		return result, nil
	}

	var deleteErrors []DeleteError
	var deletedObjects []ObjectIdentifier
	var deltaResult = make([]int64, len(StorageClassIndexMap))
	var unexpiredInfo []UnexpiredTriple
	var wg = sync.WaitGroup{}
	for _, o := range objects {
		wg.Add(1)
		go func(o ObjectIdentifier) {
			object, err := getFunc(o, tx)
			defer wg.Done()
			if err != nil && err == ErrNoSuchKey {
				deletedObjects = append(deletedObjects, ObjectIdentifier{
					ObjectName:   o.ObjectName,
					VersionId:    o.VersionId,
					DeleteMarker: o.DeleteMarker,
					DeleteMarkerVersionId: helper.Ternary(o.DeleteMarker,
						o.VersionId, "").(string),
				})
				return
			}
			var delResult DeleteObjectResult
			if err == nil {
				delResult, err = deleteFunc(object, tx)
			}
			if err == nil {
				if object.VersionId != "" {
					yig.MetaStorage.Cache.Remove(redis.ObjectTable, bucket.Name+":"+object.Name+":"+object.VersionId)
					yig.DataCache.Remove(bucket.Name + ":" + object.Name + ":" + object.VersionId)
				} else if reqCtx.ObjectInfo != nil {
					yig.MetaStorage.Cache.Remove(redis.ObjectTable, bucket.Name+":"+object.Name+":"+object.VersionId)
					yig.DataCache.Remove(bucket.Name + ":" + object.Name + ":" + object.VersionId)
				}
				deletedObjects = append(deletedObjects, ObjectIdentifier{
					ObjectName:   object.Name,
					VersionId:    object.VersionId,
					DeleteMarker: delResult.DeleteMarker,
					DeleteMarkerVersionId: helper.Ternary(delResult.DeleteMarker,
						delResult.VersionId, "").(string),
				})

				if ok, delta := object.IsUnexpired(); ok {
					unexpiredInfo = append(unexpiredInfo, UnexpiredTriple{
						StorageClass: object.StorageClass,
						Size:         CorrectDeltaSize(object.StorageClass, object.Size),
						SurvivalTime: delta,
					})
				}
				if delResult.DeltaSize.Delta != 0 {
					atomic.AddInt64(&deltaResult[delResult.DeltaSize.StorageClass], CorrectDeltaSize(object.StorageClass, object.Size))
				}
			} else {
				helper.Logger.Error("Unable to delete object:", err)
				apiErrorCode, ok := err.(ApiErrorCode)
				if ok {
					deleteErrors = append(deleteErrors, DeleteError{
						Code:      ErrorCodeResponse[apiErrorCode].AwsErrorCode,
						Message:   ErrorCodeResponse[apiErrorCode].Description,
						Key:       object.Name,
						VersionId: object.VersionId,
					})
				} else {
					deleteErrors = append(deleteErrors, DeleteError{
						Code:      "InternalError",
						Message:   "We encountered an internal error, please try again.",
						Key:       object.Name,
						VersionId: object.VersionId,
					})
				}
			}
		}(o)
	}

	wg.Wait()
	result.DeletedObjects = deletedObjects
	result.UnexpiredInfo = unexpiredInfo
	result.DeleteErrors = deleteErrors
	result.DeltaResult = deltaResult
	return
}
