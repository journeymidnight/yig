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

	"github.com/journeymidnight/yig/api"
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/backend"
	"github.com/journeymidnight/yig/crypto"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/signature"
)

var latestQueryTime [2]time.Time // 0 is for SMALL_FILE_POOLNAME, 1 is for BIG_FILE_POOLNAME
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

func (yig *YigStorage) pickClusterAndPool(bucket string, object string,
	size int64, isAppend bool) (cluster backend.Cluster, poolName string) {

	var idx int
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

	if version == "" {
		object, err = yig.MetaStorage.GetObject(bucketName, objectName, true)
	} else {
		object, err = yig.getObjWithVersion(bucketName, objectName, version)
	}
	if err != nil {
		return
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

func (yig *YigStorage) GetObjectInfoByCtx(reqCtx api.RequestContext, credential common.Credential) (object *meta.Object, err error) {
	bucket, version := reqCtx.BucketInfo, reqCtx.VersionId
	if bucket == nil {
		return nil, ErrNoSuchBucket
	}
	object = reqCtx.ObjectInfo
	if object == nil {
		return nil, ErrNoSuchKey
	}
	if version != "" {
		object, err = yig.getObjWithVersion(reqCtx.BucketName, reqCtx.ObjectName, version)
		if err != nil {
			return
		}
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

func (yig *YigStorage) GetObjectAcl(bucketName string, objectName string,
	version string, credential common.Credential) (policy datatype.AccessControlPolicyResponse, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		return
	}

	var object *meta.Object
	if version == "" {
		object, err = yig.MetaStorage.GetObject(bucketName, objectName, true)
	} else {
		object, err = yig.getObjWithVersion(bucketName, objectName, version)
	}
	if err != nil {
		return
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

func (yig *YigStorage) SetObjectAcl(bucketName string, objectName string, version string,
	policy datatype.AccessControlPolicy, acl datatype.Acl, credential common.Credential) error {

	if acl.CannedAcl == "" {
		newCannedAcl, err := datatype.GetCannedAclFromPolicy(policy)
		if err != nil {
			return err
		}
		acl = newCannedAcl
	}

	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		return err
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
	var object *meta.Object
	if version == "" {
		object, err = yig.MetaStorage.GetObject(bucketName, objectName, false)
	} else {
		object, err = yig.getObjWithVersion(bucketName, objectName, version)
	}
	if err != nil {
		return err
	}
	object.ACL = acl
	err = yig.MetaStorage.UpdateObjectAcl(object)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.ObjectTable,
			bucketName+":"+objectName+":"+version)
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
func (yig *YigStorage) PutObject(reqCtx api.RequestContext, credential common.Credential,
	size int64, data io.ReadCloser, metadata map[string]string, acl datatype.Acl,
	sseRequest datatype.SseRequest, storageClass meta.StorageClass) (result datatype.PutObjectResult, err error) {
	bucketName, objectName := reqCtx.BucketName, reqCtx.ObjectName

	defer data.Close()
	encryptionKey, cipherKey, err := yig.encryptionKeyFromSseRequest(sseRequest, bucketName, objectName)
	reqCtx.Logger.Info("get encryptionKey:", encryptionKey, "cipherKey:", cipherKey, "err:", err)
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

	cluster, poolName := yig.pickClusterAndPool(bucketName, objectName, size, false)
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
		reqCtx.Logger.Error("Failed to write objects, already written",
			bytesWritten, "total size", size)
		return result, ErrIncompleteBody
	}

	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	reqCtx.Logger.Info("CalculatedMd5:", calculatedMd5, "userMd5:", metadata["md5Sum"])
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
		NullVersion:      helper.Ternary(bucket.Versioning == "Enabled", false, true).(bool),
		DeleteMarker:     false,
		SseType:          sseRequest.Type,
		EncryptionKey: helper.Ternary(sseRequest.Type == crypto.S3.String(),
			cipherKey, []byte("")).([]byte),
		InitializationVector: initializationVector,
		CustomAttributes:     metadata,
		Type:                 meta.ObjectTypeNormal,
		StorageClass:         storageClass,
	}

	result.LastModified = object.LastModifiedTime
	var nullVerNum uint64
	nullVerNum, err = yig.checkOldObject(reqCtx)
	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	}
	if bucket.Versioning == meta.VersionEnabled {
		result.VersionId = object.GetVersionId()
	}
	// update null version number
	if bucket.Versioning == meta.VersionSuspended {
		nullVerNum = uint64(object.LastModifiedTime.UnixNano())
	}

	if nullVerNum != 0 {
		objMap := &meta.ObjMap{
			Name:       objectName,
			BucketName: bucketName,
		}
		err = yig.MetaStorage.PutObject(object, nil, objMap, true)
	} else {
		err = yig.MetaStorage.PutObject(object, nil, nil, true)
	}

	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	}

	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.ObjectTable, bucketName+":"+objectName+":")
		yig.DataCache.Remove(bucketName + ":" + objectName + ":" + object.GetVersionId())
	}
	return result, nil
}

func (yig *YigStorage) RenameObject(targetObject *meta.Object, sourceObject string, credential common.Credential) (result datatype.RenameObjectResult, err error) {

	bucket, err := yig.MetaStorage.GetBucket(targetObject.BucketName, true)
	if err != nil {
		return
	}
	switch bucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.OwnerId != credential.UserId {
			return result, ErrBucketAccessForbidden
		}
	}

	if len(targetObject.Parts) != 0 {
		err = yig.MetaStorage.RenameObjectPart(targetObject, sourceObject)
		if err != nil {
			helper.Logger.Error("Update Object Attrs, sql fails:", err)
			return result, ErrInternalError
		}
	} else {
		err = yig.MetaStorage.RenameObject(targetObject, sourceObject)
		if err != nil {
			helper.Logger.Error("Update Object Attrs, sql fails:", err)
			return result, ErrInternalError
		}
	}

	result.LastModified = targetObject.LastModifiedTime
	yig.MetaStorage.Cache.Remove(redis.ObjectTable, targetObject.BucketName+":"+targetObject.Name+":")
	yig.DataCache.Remove(targetObject.BucketName + ":" + targetObject.Name + ":" + targetObject.GetVersionId())

	return result, nil
}

func (yig *YigStorage) CopyObject(reqCtx api.RequestContext, targetObject *meta.Object, source io.Reader, credential common.Credential,
	sseRequest datatype.SseRequest) (result datatype.PutObjectResult, err error) {

	var oid string
	var maybeObjectToRecycle objectToRecycle
	var encryptionKey []byte
	encryptionKey, cipherKey, err := yig.encryptionKeyFromSseRequest(sseRequest, targetObject.BucketName, targetObject.Name)
	if err != nil {
		return
	}

	bucket, err := yig.MetaStorage.GetBucket(targetObject.BucketName, true)
	if err != nil {
		return
	}

	switch bucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.OwnerId != credential.UserId {
			return result, ErrBucketAccessForbidden
		}
	}

	// Limit the reader to its provided size if specified.
	var limitedDataReader io.Reader
	limitedDataReader = io.LimitReader(source, targetObject.Size)

	cephCluster, poolName := yig.pickClusterAndPool(targetObject.BucketName,
		targetObject.Name, targetObject.Size, false)

	if len(targetObject.Parts) != 0 {
		var targetParts map[int]*meta.Part = make(map[int]*meta.Part, len(targetObject.Parts))
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

	targetObject.Rowkey = nil   // clear the rowkey cache
	targetObject.VersionId = "" // clear the versionId cache
	targetObject.Location = cephCluster.ID()
	targetObject.Pool = poolName
	targetObject.OwnerId = credential.UserId
	targetObject.LastModifiedTime = time.Now().UTC()
	targetObject.NullVersion = helper.Ternary(bucket.Versioning == "Enabled", false, true).(bool)
	targetObject.DeleteMarker = false
	targetObject.SseType = sseRequest.Type
	targetObject.EncryptionKey = helper.Ternary(sseRequest.Type == crypto.S3.String(),
		cipherKey, []byte("")).([]byte)

	result.LastModified = targetObject.LastModifiedTime

	var nullVerNum uint64
	nullVerNum, err = yig.checkOldObject(reqCtx)
	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	}
	if bucket.Versioning == "Enabled" {
		result.VersionId = targetObject.GetVersionId()
	}
	// update null version number
	if bucket.Versioning == "Suspended" {
		nullVerNum = uint64(targetObject.LastModifiedTime.UnixNano())
	}

	objMap := &meta.ObjMap{
		Name:       targetObject.Name,
		BucketName: targetObject.BucketName,
	}

	if nullVerNum != 0 {
		objMap.NullVerNum = nullVerNum
		err = yig.MetaStorage.PutObject(targetObject, nil, objMap, true)
	} else {
		err = yig.MetaStorage.PutObject(targetObject, nil, nil, true)
	}

	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	}

	yig.MetaStorage.Cache.Remove(redis.ObjectTable, targetObject.BucketName+":"+targetObject.Name+":")
	yig.DataCache.Remove(targetObject.BucketName + ":" + targetObject.Name + ":" + targetObject.GetVersionId())

	return result, nil
}

func (yig *YigStorage) removeByObject(object *meta.Object, objMap *meta.ObjMap) (err error) {
	if object == nil {
		return nil
	}
	err = yig.MetaStorage.DeleteObject(object, object.DeleteMarker, objMap)
	if err != nil {
		return
	}
	return nil
}

func (yig *YigStorage) removeObject(bucket *meta.Bucket, object *meta.Object, version string) (result datatype.DeleteObjectResult, err error) {
	if bucket.Versioning == meta.VersionDisabled {
		return result, yig.MetaStorage.DeleteObject(object, object.DeleteMarker, nil)
	} else if version == "" {
		result.VersionId, err = yig.addDeleteMarker(*bucket, object.Name, false)
		if err != nil {
			return result, err
		}
		result.DeleteMarker = true
		return result, nil
	} else if object.DeleteMarker {
		result.DeleteMarker = true
		result.VersionId = object.VersionId
		return result, nil
	} else {
		result.VersionId = version
		return result, yig.removeObjectVersion(bucket.Name, object.Name, version)
	}
}

func (yig *YigStorage) getObjWithVersion(bucketName, objectName, version string) (object *meta.Object, err error) {
	if version == "null" {
		objMap, err := yig.MetaStorage.GetObjectMap(bucketName, objectName)
		if err != nil {
			return nil, err
		}
		version = objMap.NullVerId
	}
	return yig.MetaStorage.GetObjectVersion(bucketName, objectName, version, true)

}

func (yig *YigStorage) checkOldObject(reqCtx api.RequestContext) (version uint64, err error) {
	bucketName, objectName := reqCtx.BucketName, reqCtx.ObjectName
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return 0, ErrNoSuchBucket
	}
	versioning := bucket.Versioning
	if versioning == meta.VersionDisabled {
		err = yig.removeByObject(reqCtx.ObjectInfo, nil)
		return
	}

	if versioning == meta.VersionEnabled || versioning == meta.VersionSuspended {
		objMapExist := true
		objectExist := true

		var objMap *meta.ObjMap
		objMap, err = yig.MetaStorage.GetObjectMap(bucketName, objectName)
		if err == ErrNoSuchKey {
			err = nil
			objMapExist = false
		} else if err != nil {
			return 0, err
		}
		var object *meta.Object
		if objMapExist {
			object, err = yig.MetaStorage.GetObjectVersion(bucketName, objectName, objMap.NullVerId, false)
			if err == ErrNoSuchKey {
				err = nil
				objectExist = false
			} else if err != nil {
				return 0, err
			}
		} else {
			object, err = yig.MetaStorage.GetObject(bucketName, objectName, false)
			if err == ErrNoSuchKey {
				err = nil
				objectExist = false
			} else if err != nil {
				return 0, err
			}
		}

		if versioning == "Enabled" {
			if !objMapExist && objectExist && object.NullVersion {
				version, err = object.GetVersionNumber()
				if err != nil {
					reqCtx.Logger.Error("GetVersionNumber error:", err)
					return 0, err
				}
				reqCtx.Logger.Info("Old object version:", version)
				return
			}
		} else {
			reqCtx.Logger.Info("object.NullVersion:", object.NullVersion)
			if objectExist && object.NullVersion {
				err = yig.MetaStorage.DeleteObject(object, object.DeleteMarker, nil)
				if err != nil {
					return
				}
			}
		}
		return
	}

	return 0, errors.New("No Such versioning status!")
}

func (yig *YigStorage) removeObjectVersion(bucketName, objectName, version string) error {
	object, err := yig.getObjWithVersion(bucketName, objectName, version)
	if err == ErrNoSuchKey {
		return nil
	}
	if err != nil {
		return err
	}

	if version == "null" {
		objMap := &meta.ObjMap{
			Name:       objectName,
			BucketName: bucketName,
		}
		err = yig.removeByObject(object, objMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (yig *YigStorage) addDeleteMarker(bucket meta.Bucket, objectName string,
	nullVersion bool) (versionId string, err error) {

	deleteMarker := &meta.Object{
		Name:             objectName,
		BucketName:       bucket.Name,
		OwnerId:          bucket.OwnerId,
		LastModifiedTime: time.Now().UTC(),
		NullVersion:      nullVersion,
		DeleteMarker:     true,
	}

	versionId = deleteMarker.GetVersionId()
	objMap := &meta.ObjMap{
		Name:       objectName,
		BucketName: bucket.Name,
	}

	if nullVersion {
		err = yig.MetaStorage.PutObject(deleteMarker, nil, objMap, false)
	} else {
		err = yig.MetaStorage.PutObject(deleteMarker, nil, nil, false)
	}

	return
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
func (yig *YigStorage) DeleteObject(reqCtx api.RequestContext,
	credential common.Credential) (result datatype.DeleteObjectResult, err error) {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return result, ErrNoSuchBucket
	}
	bucketName, objectName, version := reqCtx.BucketName, reqCtx.ObjectName, reqCtx.VersionId
	switch bucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.OwnerId != credential.UserId && credential.UserId != "" {
			return result, ErrBucketAccessForbidden
		}
	} // TODO policy and fancy ACL

	switch bucket.Versioning {
	case meta.VersionDisabled:
		if version != "" && version != "null" {
			return result, ErrNoSuchVersion
		}
		err = yig.removeByObject(reqCtx.ObjectInfo, nil)
		if err != nil {
			return
		}
	case meta.VersionEnabled:
		if version == "" {
			result.VersionId, err = yig.addDeleteMarker(*bucket, objectName, false)
			if err != nil {
				return
			}
			result.DeleteMarker = true
		} else {
			err = yig.removeObjectVersion(bucketName, objectName, version)
			if err != nil {
				return
			}
			result.VersionId = version
		}
	case meta.VersionSuspended:
		if version == "" {
			err = yig.removeObjectVersion(bucketName, objectName, "null")
			if err != nil {
				return
			}
			result.VersionId, err = yig.addDeleteMarker(*bucket, objectName, true)
			if err != nil {
				return
			}
			result.DeleteMarker = true
		} else {
			err = yig.removeObjectVersion(bucketName, objectName, version)
			if err != nil {
				return
			}
			result.VersionId = version
		}
	default:
		reqCtx.Logger.Error("Invalid bucket versioning:", bucketName)
		return result, ErrInternalError
	}

	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.ObjectTable, bucketName+":"+objectName+":")
		yig.DataCache.Remove(bucketName + ":" + objectName + ":")
		yig.DataCache.Remove(bucketName + ":" + objectName + ":" + "null")
		if version != "" {
			yig.MetaStorage.Cache.Remove(redis.ObjectTable,
				bucketName+":"+objectName+":"+version)
			yig.DataCache.Remove(bucketName + ":" + objectName + ":" + version)
		}
	}
	return result, nil
}

func (yig *YigStorage) DeleteMultipleObjects(reqCtx api.RequestContext, objects []datatype.ObjectIdentifier,
	credential common.Credential) (result datatype.DeleteMultipleObjectsResult, err error) {
	logger := reqCtx.Logger
	logger.Info("Enter DeleteMultipleObjects")
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return result, ErrNoSuchBucket
	}
	switch bucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.OwnerId != credential.UserId && credential.UserId != "" {
			return result, ErrBucketAccessForbidden
		}
	} // TODO policy and fancy ACL

	var deleteObjects []datatype.ObjectIdentifier
	var deleteObjectsWithVersion []datatype.ObjectIdentifier
	for _, o := range objects {
		if o.VersionId != "" {
			deleteObjectsWithVersion = append(deleteObjectsWithVersion, o)
		} else {
			deleteObjects = append(deleteObjects, o)
		}
	}
	if bucket.Versioning == meta.VersionDisabled && len(deleteObjectsWithVersion) != 0 {
		return result, ErrNotSupportEnabledBucketVersion
	}

	// WARNING: Suppose the all objects in the request exists,
	// otherwise the result of the object will not be returned if the object does not exist.
	objs, err := yig.MetaStorage.GetDeleteObjects(*bucket, deleteObjects, false)
	if err != nil {
		return result, err
	}
	objsWithVersion, err := yig.MetaStorage.GetDeleteObjects(*bucket, deleteObjectsWithVersion, true)
	if err != nil {
		return result, err
	}
	for _, o := range objs {
		r, err := yig.removeObject(bucket, o, "")
		if err == nil {
			result.DeletedObjects = append(result.DeletedObjects, datatype.ObjectIdentifier{
				ObjectName:            o.Name,
				DeleteMarker:          r.DeleteMarker,
				DeleteMarkerVersionId: r.VersionId,
			})
		} else {
			logger.Error("Unable to delete object:", err)
			apiErrorCode, ok := err.(ApiErrorCode)
			if ok {
				result.DeleteErrors = append(result.DeleteErrors, datatype.DeleteError{
					Code:    ErrorCodeResponse[apiErrorCode].AwsErrorCode,
					Message: ErrorCodeResponse[apiErrorCode].Description,
					Key:     o.Name,
				})
			} else {
				result.DeleteErrors = append(result.DeleteErrors, datatype.DeleteError{
					Code:    "InternalError",
					Message: "We encountered an internal error, please try again.",
					Key:     o.Name,
				})
			}
		}
	}

	for _, o := range objsWithVersion {
		r, err := yig.removeObject(bucket, o, o.VersionId)
		if err == nil {
			result.DeletedObjects = append(result.DeletedObjects, datatype.ObjectIdentifier{
				ObjectName:            o.Name,
				VersionId:             o.VersionId,
				DeleteMarker:          r.DeleteMarker,
				DeleteMarkerVersionId: r.VersionId,
			})
		} else {
			logger.Error("Unable to delete object:", err)
			apiErrorCode, ok := err.(ApiErrorCode)
			if ok {
				result.DeleteErrors = append(result.DeleteErrors, datatype.DeleteError{
					Code:      ErrorCodeResponse[apiErrorCode].AwsErrorCode,
					Message:   ErrorCodeResponse[apiErrorCode].Description,
					Key:       o.Name,
					VersionId: o.VersionId,
				})
			} else {
				result.DeleteErrors = append(result.DeleteErrors, datatype.DeleteError{
					Code:      "InternalError",
					Message:   "We encountered an internal error, please try again.",
					Key:       o.Name,
					VersionId: o.VersionId,
				})
			}
		}
	}
	return
}
