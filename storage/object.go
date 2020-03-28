package storage

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
	"path"
	"sync"
	"time"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/backend"
	. "github.com/journeymidnight/yig/context"
	"github.com/journeymidnight/yig/crypto"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/signature"
)

var cMap sync.Map
var latestQueryTime [3]time.Time // 0 is for SMALL_FILE_POOLNAME, 1 is for BIG_FILE_POOLNAME, 2 is for GLACIER_FILE_POOLNAME
const (
	CLUSTER_MAX_USED_SPACE_PERCENT = 85
	BIG_FILE_THRESHOLD             = 128 << 10 /* 128K */
)

func (yig *YigStorage) pickRandomCluster() (cluster backend.Cluster) {
	helper.Logger.Warn("Error picking cluster from table cluster in DB, " +
		"use first cluster in config to write.")
	for _, c := range yig.DataStorage {
		cluster = c
		break
	}
	return
}

func (yig *YigStorage) pickClusterAndPool(bucket string, object string, storageClass meta.StorageClass,
	size int64, isAppend bool) (cluster backend.Cluster, poolName string) {

	var idx int
	if storageClass == meta.ObjectStorageClassGlacier {
		poolName = backend.GLACIER_FILE_POOLNAME
		idx = 2
	} else {
		if isAppend {
			poolName = backend.BIG_FILE_POOLNAME
			idx = 1
		} else if size < 0 { // request.ContentLength is -1 if length is unknown
			poolName = backend.BIG_FILE_POOLNAME
			idx = 1
		} else if size < BIG_FILE_THRESHOLD {
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
		reader, err := cluster.GetReader(object.Pool, object.ObjectId,
			0, uint64(object.Size))
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
func getAlignedReader(cluster backend.Cluster, poolName, objectName string,
	startOffset int64, length uint64) (reader io.ReadCloser, err error) {

	alignedOffset := startOffset / AES_BLOCK_SIZE * AES_BLOCK_SIZE
	length += uint64(startOffset - alignedOffset)
	return cluster.GetReader(poolName, objectName, alignedOffset, length)
}

func (yig *YigStorage) GetObject(object *meta.Object, startOffset int64,
	length int64, writer io.Writer, sseRequest datatype.SseRequest) (err error) {
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
			return getAlignedReader(cephCluster, object.Pool, object.ObjectId,
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

	reader, err := getAlignedReader(cluster, pool, part.ObjectId,
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

func (yig *YigStorage) GetObjectInfo(bucketName string, objectName string,
	version string, credential common.Credential) (object *meta.Object, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		return
	}

	if bucket.Versioning == datatype.BucketVersioningDisabled {
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

	if !credential.AllowOtherUserAccess {
		switch object.ACL.CannedAcl {
		case "public-read", "public-read-write":
			break
		case "authenticated-read":
			if credential.UserId == "" {
				err = ErrAccessDenied
				return
			}
		case "bucket-owner-read", "bucket-owner-full-control":
			if bucket.OwnerId != credential.UserId {
				err = ErrAccessDenied
				return
			}
		default:
			if object.OwnerId != credential.UserId {
				err = ErrAccessDenied
				return
			}
		}
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

	if !credential.AllowOtherUserAccess {
		switch object.ACL.CannedAcl {
		case "public-read", "public-read-write":
			break
		case "authenticated-read":
			if credential.UserId == "" {
				err = ErrAccessDenied
				return
			}
		case "bucket-owner-read", "bucket-owner-full-control":
			if bucket.OwnerId != credential.UserId {
				err = ErrAccessDenied
				return
			}
		default:
			if object.OwnerId != credential.UserId {
				err = ErrAccessDenied
				return
			}
		}
	}
	return
}

func (yig *YigStorage) GetObjectAcl(reqCtx RequestContext, credential common.Credential) (
	policy datatype.AccessControlPolicyResponse, err error) {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return policy, ErrNoSuchBucket
	}

	object := reqCtx.ObjectInfo
	if object == nil {
		return policy, ErrNoSuchKey
	}

	switch object.ACL.CannedAcl {
	case "bucket-owner-full-control":
		if bucket.OwnerId != credential.UserId {
			err = ErrAccessDenied
			return
		}
	default:
		if object.OwnerId != credential.UserId {
			err = ErrAccessDenied
			return
		}
	}

	owner := datatype.Owner{ID: credential.UserId, DisplayName: credential.DisplayName}
	bucketCred, err := iam.GetCredentialByUserId(bucket.OwnerId)
	if err != nil {
		return
	}
	bucketOwner := datatype.Owner{ID: bucketCred.UserId, DisplayName: bucketCred.DisplayName}
	policy, err = datatype.CreatePolicyFromCanned(owner, bucketOwner, object.ACL)
	if err != nil {
		return
	}

	return
}

func (yig *YigStorage) SetObjectAcl(reqCtx RequestContext, policy datatype.AccessControlPolicy, acl datatype.Acl,
	credential common.Credential) error {

	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}

	object := reqCtx.ObjectInfo
	if object == nil {
		return ErrNoSuchKey
	}

	if acl.CannedAcl == "" {
		newCannedAcl, err := datatype.GetCannedAclFromPolicy(policy)
		if err != nil {
			return err
		}
		acl = newCannedAcl
	}

	switch bucket.ACL.CannedAcl {
	case "bucket-owner-full-control":
		if bucket.OwnerId != credential.UserId {
			return ErrAccessDenied
		}
	default:
		if bucket.OwnerId != credential.UserId {
			return ErrAccessDenied
		}
	} // TODO policy and fancy ACL

	object.ACL = acl
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
	size int64, data io.ReadCloser, metadata map[string]string, acl datatype.Acl,
	sseRequest datatype.SseRequest, storageClass meta.StorageClass) (result datatype.PutObjectResult, err error) {
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

	switch bucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.OwnerId != credential.UserId {
			return result, ErrBucketAccessForbidden
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
	objectId, bytesWritten, err := cluster.Put(poolName, storageReader)
	if err != nil {
		return
	}
	// Should metadata update failed, add `maybeObjectToRecycle` to `RecycleQueue`,
	// so the object in Ceph could be removed asynchronously
	maybeObjectToRecycle := objectToRecycle{
		location: cluster.ID(),
		pool:     poolName,
		objectId: objectId,
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
	// TODO validate bucket policy and fancy ACL
	object := &meta.Object{
		Name:             objectName,
		BucketName:       bucketName,
		Location:         cluster.ID(),
		Pool:             poolName,
		OwnerId:          credential.UserId,
		Size:             int64(bytesWritten),
		ObjectId:         objectId,
		LastModifiedTime: time.Now().UTC(),
		Etag:             calculatedMd5,
		ContentType:      metadata["Content-Type"],
		ACL:              acl,
		NullVersion:      helper.Ternary(bucket.Versioning == datatype.BucketVersioningEnabled, false, true).(bool),
		DeleteMarker:     false,
		SseType:          sseRequest.Type,
		EncryptionKey: helper.Ternary(sseRequest.Type == crypto.S3.String(),
			cipherKey, []byte("")).([]byte),
		InitializationVector: initializationVector,
		CustomAttributes:     metadata,
		Type:                 meta.ObjectTypeNormal,
		StorageClass:         storageClass,
	}
	object.VersionId = object.GenVersionId(bucket.Versioning)
	if object.StorageClass == meta.ObjectStorageClassGlacier {
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
	if object.VersionId != meta.NullVersion {
		result.VersionId = object.VersionId
	}
	err = yig.MetaStorage.PutObject(reqCtx, object, nil, true)
	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	} else {
		yig.MetaStorage.Cache.Remove(redis.ObjectTable, bucketName+":"+objectName+":"+object.VersionId)
		yig.DataCache.Remove(bucketName + ":" + objectName + ":" + object.VersionId)
		if reqCtx.ObjectInfo != nil && reqCtx.BucketInfo.Versioning != datatype.BucketVersioningEnabled {
			go yig.removeOldObject(reqCtx.ObjectInfo)
		}
	}

	return result, nil
}

func (yig *YigStorage) PutObjectMeta(bucket *meta.Bucket, targetObject *meta.Object, credential common.Credential) (err error) {
	switch bucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.OwnerId != credential.UserId {
			return ErrBucketAccessForbidden
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

func (yig *YigStorage) RenameObject(reqCtx RequestContext, targetObject *meta.Object, sourceObject string, credential common.Credential) (result datatype.RenameObjectResult, err error) {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return result, ErrNoSuchBucket
	}
	switch bucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.OwnerId != credential.UserId {
			return result, ErrBucketAccessForbidden
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
	sseRequest datatype.SseRequest, isMetadataOnly bool) (result datatype.PutObjectResult, err error) {
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

	switch targetBucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if targetBucket.OwnerId != credential.UserId {
			return result, ErrBucketAccessForbidden
		}
	}
	targetObject.LastModifiedTime = time.Now().UTC()
	targetObject.VersionId = targetObject.GenVersionId(targetBucket.Versioning)
	if isMetadataOnly {
		if sourceObject.StorageClass == meta.ObjectStorageClassGlacier {
			err = yig.MetaStorage.UpdateGlacierObject(reqCtx, targetObject, sourceObject, true)
			if err != nil {
				helper.Logger.Error("Copy Object with same source and target with GLACIER object, sql fails:", err)
				return result, ErrInternalError
			}
		} else {
			err = yig.MetaStorage.ReplaceObjectMetas(targetObject)
			if err != nil {
				helper.Logger.Error("Copy Object with same source and target, sql fails:", err)
				return result, ErrInternalError
			}
		}

		result.LastModified = targetObject.LastModifiedTime
		if targetBucket.Versioning == datatype.BucketVersioningEnabled {
			result.VersionId = targetObject.VersionId
		}
		yig.MetaStorage.Cache.Remove(redis.ObjectTable, targetObject.BucketName+":"+targetObject.Name+":"+targetObject.VersionId)
		yig.DataCache.Remove(targetObject.BucketName + ":" + targetObject.Name + ":" + targetObject.VersionId)
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
			result, err = func() (result datatype.PutObjectResult, err error) {
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
				oid, bytesW, err = cephCluster.Put(poolName, storageReader)
				maybeObjectToRecycle = objectToRecycle{
					location: cephCluster.ID(),
					pool:     poolName,
					objectId: oid,
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
		oid, bytesWritten, err = cephCluster.Put(poolName, storageReader)
		if err != nil {
			return
		}
		// Should metadata update failed, add `maybeObjectToRecycle` to `RecycleQueue`,
		// so the object in Ceph could be removed asynchronously
		maybeObjectToRecycle = objectToRecycle{
			location: cephCluster.ID(),
			pool:     poolName,
			objectId: oid,
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
	targetObject.OwnerId = credential.UserId
	targetObject.NullVersion = helper.Ternary(targetBucket.Versioning == datatype.BucketVersioningEnabled, false, true).(bool)
	targetObject.DeleteMarker = false
	targetObject.SseType = sseRequest.Type
	targetObject.EncryptionKey = helper.Ternary(sseRequest.Type == crypto.S3.String(),
		cipherKey, []byte("")).([]byte)

	result.LastModified = targetObject.LastModifiedTime
	if targetObject.StorageClass == meta.ObjectStorageClassGlacier && targetObject.Name == sourceObject.Name && targetObject.BucketName == sourceObject.BucketName {
		targetObject.LastModifiedTime = sourceObject.LastModifiedTime
		result.LastModified = targetObject.LastModifiedTime
		err = yig.MetaStorage.UpdateGlacierObject(reqCtx, targetObject, sourceObject, false)
	} else {
		err = yig.MetaStorage.PutObject(reqCtx, targetObject, nil, true)
	}
	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	} else {
		yig.MetaStorage.Cache.Remove(redis.ObjectTable, targetObject.BucketName+":"+targetObject.Name+":"+targetObject.VersionId)
		yig.DataCache.Remove(targetObject.BucketName + ":" + targetObject.Name + ":" + targetObject.VersionId)
		if reqCtx.ObjectInfo != nil && reqCtx.BucketInfo.Versioning != datatype.BucketVersioningEnabled {
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

	if object.StorageClass == meta.ObjectStorageClassGlacier {
		freezer, err := yig.GetFreezer(object.BucketName, object.Name, "")
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
func (yig *YigStorage) AppendObject(bucketName string, objectName string, credential common.Credential,
	offset uint64, size int64, data io.ReadCloser, metadata map[string]string, acl datatype.Acl,
	sseRequest datatype.SseRequest, storageClass meta.StorageClass, objInfo *meta.Object) (result datatype.AppendObjectResult, err error) {

	defer data.Close()
	encryptionKey, cipherKey, err := yig.encryptionKeyFromSseRequest(sseRequest, bucketName, objectName)
	helper.Logger.Println(10, "get encryptionKey:", encryptionKey, "cipherKey:", cipherKey, "err:", err)
	if err != nil {
		return
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
		poolName = backend.BIG_FILE_POOLNAME
		oid = objInfo.ObjectId
		initializationVector = objInfo.InitializationVector
		objSize = objInfo.Size
		storageClass = objInfo.StorageClass
		helper.Logger.Println(20, "request append oid:", oid, "iv:", initializationVector, "size:", objSize)
	} else {
		// New appendable object
		cephCluster, poolName = yig.pickClusterAndPool(bucketName, objectName, storageClass, size, true)
		if cephCluster == nil || poolName != backend.BIG_FILE_POOLNAME {
			helper.Logger.Warn("PickOneClusterAndPool error")
			return result, ErrInternalError
		}
		if len(encryptionKey) != 0 {
			initializationVector, err = newInitializationVector()
			if err != nil {
				return
			}
		}
		helper.Logger.Println(20, "request first append oid:", oid, "iv:", initializationVector, "size:", objSize)
	}

	dataReader := io.TeeReader(limitedDataReader, md5Writer)

	storageReader, err := wrapEncryptionReader(dataReader, encryptionKey, initializationVector)
	if err != nil {
		return
	}
	oid, bytesWritten, err := cephCluster.Append(poolName, oid, storageReader, int64(offset))
	if err != nil {
		helper.Logger.Error("cephCluster.Append err:", err, poolName, oid, offset)
		return
	}

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
	object := &meta.Object{
		Name:                 objectName,
		BucketName:           bucketName,
		Location:             cephCluster.ID(),
		Pool:                 poolName,
		OwnerId:              credential.UserId,
		Size:                 objSize + int64(bytesWritten),
		ObjectId:             oid,
		LastModifiedTime:     time.Now().UTC(),
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
	}

	result.LastModified = object.LastModifiedTime
	result.NextPosition = object.Size
	helper.Logger.Println(20, "Append info.", "bucket:", bucketName, "objName:", objectName, "oid:", oid,
		"objSize:", object.Size, "bytesWritten:", bytesWritten, "storageClass:", storageClass)
	err = yig.MetaStorage.AppendObject(object, objInfo != nil)
	if err != nil {
		return
	}

	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.ObjectTable, bucketName+":"+objectName+":"+meta.NullVersion)
		yig.DataCache.Remove(bucketName + ":" + objectName + ":" + object.VersionId)
	}
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
	credential common.Credential) (result datatype.DeleteObjectResult, err error) {

	bucket, object := reqCtx.BucketInfo, reqCtx.ObjectInfo
	if bucket == nil {
		return result, ErrNoSuchBucket
	}

	bucketName, objectName, reqVersion := reqCtx.BucketName, reqCtx.ObjectName, reqCtx.VersionId
	switch bucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.OwnerId != credential.UserId && credential.UserId != "" {
			return result, ErrBucketAccessForbidden
		}
	} // TODO policy and fancy ACL

	switch bucket.Versioning {
	case datatype.BucketVersioningDisabled:
		if reqVersion != "" && reqVersion != meta.NullVersion {
			return result, ErrNoSuchVersion
		}
		if object == nil {
			return result, ErrNoSuchKey
		}
		err = yig.MetaStorage.DeleteObject(object)
		if err != nil {
			return
		}
	case datatype.BucketVersioningEnabled:
		if reqVersion != "" {
			if object == nil {
				return result, ErrNoSuchKey
			}
			err = yig.MetaStorage.DeleteObject(object)
			if err != nil {
				return
			}
			if object.DeleteMarker {
				result.DeleteMarker = true
			}
			result.VersionId = object.VersionId

		} else {
			// Add delete marker                                    |
			if object == nil {
				object = &meta.Object{
					BucketName: bucketName,
					Name:       objectName,
					OwnerId:    credential.UserId,
				}
			}
			object.DeleteMarker = true
			object.LastModifiedTime = time.Now().UTC()
			object.VersionId = object.GenVersionId(bucket.Versioning)
			err = yig.MetaStorage.AddDeleteMarker(object)
			if err != nil {
				return
			}
			result.VersionId = object.VersionId
		}
	case datatype.BucketVersioningSuspended:
		if reqVersion != "" {
			if object == nil {
				return result, ErrNoSuchKey
			}
			err = yig.MetaStorage.DeleteObject(object)
			if err != nil {
				return
			}
			if object.DeleteMarker {
				result.DeleteMarker = true
			}
			result.VersionId = object.VersionId
		} else {
			nullVersionExist := (object != nil)
			if !nullVersionExist {
				object = &meta.Object{
					BucketName:       bucketName,
					Name:             objectName,
					OwnerId:          credential.UserId,
					DeleteMarker:     true,
					LastModifiedTime: time.Now().UTC(),
					VersionId:        meta.NullVersion,
				}
				err = yig.MetaStorage.AddDeleteMarker(object)
				if err != nil {
					return
				}
			} else {
				err = yig.MetaStorage.DeleteSuspendedObject(object)
				if err != nil {
					return
				}
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
