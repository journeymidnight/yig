package storage

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
	"time"

	"path"
	"sync"

	"github.com/journeymidnight/yig/api/datatype"
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
const CLUSTER_MAX_USED_SPACE_PERCENT = 85

func (yig *YigStorage) PickOneClusterAndPool(bucket string, object string, size int64, isAppend bool) (cluster *CephStorage,
	poolName string) {

	var idx int
	if isAppend {
		poolName = BIG_FILE_POOLNAME
		idx = 1
	} else if size < 0 { // request.ContentLength is -1 if length is unknown
		poolName = BIG_FILE_POOLNAME
		idx = 1
	} else if size < BIG_FILE_THRESHOLD {
		poolName = SMALL_FILE_POOLNAME
		idx = 0
	} else {
		poolName = BIG_FILE_POOLNAME
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
	for fsid, _ := range yig.DataStorage {
		cluster, err := yig.MetaStorage.GetCluster(fsid, poolName)
		if err != nil {
			helper.Debugln("Error getting cluster: ", err)
			continue
		}
		if cluster.Weight == 0 {
			continue
		}
		if needCheck {
			pct, err := yig.DataStorage[fsid].GetUsedSpacePercent()
			if err != nil {
				helper.Logger.Println(0, "Error getting used space: ", err, "fsid: ", fsid)
				continue
			}
			if pct > CLUSTER_MAX_USED_SPACE_PERCENT {
				helper.Logger.Println(0, "Cluster used space exceed ", CLUSTER_MAX_USED_SPACE_PERCENT, fsid)
				continue
			}
		}
		totalWeight += cluster.Weight
		clusterWeights[fsid] = cluster.Weight
	}
	if len(clusterWeights) == 0 || totalWeight == 0 {
		helper.Logger.Println(5, "Error picking cluster from table cluster in DB! Use first cluster in config to write.")
		for _, c := range yig.DataStorage {
			cluster = c
			break
		}
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

func (yig *YigStorage) GetClusterByFsName(fsName string) (cluster *CephStorage, err error) {
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
		return make([]byte, MIN_CHUNK_SIZE)
	}
}

func generateTransWholeObjectFunc(cephCluster *CephStorage, object *meta.Object) func(io.Writer) error {
	getWholeObject := func(w io.Writer) error {
		reader, err := cephCluster.getReader(object.Pool, object.ObjectId, 0, object.Size)
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

func generateTransPartObjectFunc(cephCluster *CephStorage, object *meta.Object, part *meta.Part, offset, length int64) func(io.Writer) error {
	getNormalObject := func(w io.Writer) error {
		var oid string
		/* the transfered part could be Part or Object */
		if part != nil {
			oid = part.ObjectId
		} else {
			oid = object.ObjectId
		}
		reader, err := cephCluster.getReader(object.Pool, oid, offset, length)
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
			transPartObjectWriter := generateTransPartObjectFunc(cephCluster, object, nil, startOffset, length)

			return yig.DataCache.WriteFromCache(object, startOffset, length, writer,
				transPartObjectWriter, transWholeObjectWriter)
		}

		// encrypted object
		normalAligenedGet := func() (io.ReadCloser, error) {
			return cephCluster.getAlignedReader(object.Pool, object.ObjectId,
				startOffset, length)
		}
		reader, err := yig.DataCache.GetAlignedReader(object, startOffset, length, normalAligenedGet,
			transWholeObjectWriter)
		if err != nil {
			return err
		}
		defer reader.Close()

		decryptedReader, err := wrapAlignedEncryptionReader(reader, startOffset, encryptionKey,
			object.InitializationVector)
		if err != nil {
			return err
		}
		buffer := make([]byte, MAX_CHUNK_SIZE)
		_, err = io.CopyBuffer(writer, decryptedReader, buffer)
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
				readLength = p.Offset + p.Size - readOffset
			} else {
				readLength = startOffset + length - (p.Offset + readOffset)
			}
			cephCluster, ok := yig.DataStorage[object.Location]
			if !ok {
				return errors.New("Cannot find specified ceph cluster: " +
					object.Location)
			}
			if object.SseType == "" { // unencrypted object

				transPartFunc := generateTransPartObjectFunc(cephCluster, object, p, readOffset, readLength)
				err := transPartFunc(writer)
				if err != nil {
					return nil
				}
				continue
			}

			// encrypted object
			err = copyEncryptedPart(object.Pool, p, cephCluster, readOffset, readLength, encryptionKey, writer)
			if err != nil {
				helper.Debugln("Multipart uploaded object write error:", err)
			}
		}
	}
	return
}

func copyEncryptedPart(pool string, part *meta.Part, cephCluster *CephStorage, readOffset int64, length int64,
	encryptionKey []byte, targetWriter io.Writer) (err error) {

	reader, err := cephCluster.getAlignedReader(pool, part.ObjectId,
		readOffset, length)
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

//func (yig *YigStorage) delTableEntryForRollback(object *meta.Object, objMap *meta.ObjMap) error {
//	if object != nil {
//		err := yig.MetaStorage.Client.DeleteObject(object)
//		return err
//	}
//
//	if objMap != nil {
//		err := yig.MetaStorage.Client.DeleteObjectMap(objMap)
//		return err
//	}
//	return nil
//}

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
func (yig *YigStorage) PutObject(bucketName string, objectName string, credential common.Credential,
	size int64, data io.Reader, metadata map[string]string, acl datatype.Acl,
	sseRequest datatype.SseRequest, storageClass meta.StorageClass) (result datatype.PutObjectResult, err error) {

	encryptionKey, cipherKey, err := yig.encryptionKeyFromSseRequest(sseRequest, bucketName, objectName)
	helper.Debugln("get encryptionKey:", encryptionKey, "cipherKey:", cipherKey, "err:", err)
	if err != nil {
		return
	}

	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		helper.Debugln("get bucket", bucket, "err:", err)
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

	md5Writer := md5.New()

	// Limit the reader to its provided size if specified.
	var limitedDataReader io.Reader
	if size > 0 { // request.ContentLength is -1 if length is unknown
		limitedDataReader = io.LimitReader(data, size)
	} else {
		limitedDataReader = data
	}

	cephCluster, poolName := yig.PickOneClusterAndPool(bucketName, objectName, size, false)
	if cephCluster == nil {
		return result, ErrInternalError
	}

	// Mapping a shorter name for the object
	oid := cephCluster.GetUniqUploadName()
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
	bytesWritten, err := cephCluster.Put(poolName, oid, storageReader)
	if err != nil {
		return
	}
	// Should metadata update failed, add `maybeObjectToRecycle` to `RecycleQueue`,
	// so the object in Ceph could be removed asynchronously
	maybeObjectToRecycle := objectToRecycle{
		location: cephCluster.Name,
		pool:     poolName,
		objectId: oid,
	}
	if bytesWritten < size {
		RecycleQueue <- maybeObjectToRecycle
		helper.Logger.Printf(2, "failed to write objects, already written(%d), total size(%d)", bytesWritten, size)
		return result, ErrIncompleteBody
	}

	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	helper.Logger.Println(20, "### calculatedMd5:", calculatedMd5, "userMd5:", metadata["md5Sum"])
	if userMd5, ok := metadata["md5Sum"]; ok {
		if userMd5 != "" && userMd5 != calculatedMd5 {
			RecycleQueue <- maybeObjectToRecycle
			return result, ErrBadDigest
		}
	}

	result.Md5 = calculatedMd5

	if signVerifyReader, ok := data.(*signature.SignVerifyReader); ok {
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
		Location:         cephCluster.Name,
		Pool:             poolName,
		OwnerId:          credential.UserId,
		Size:             bytesWritten,
		ObjectId:         oid,
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
	nullVerNum, err = yig.checkOldObject(bucketName, objectName, bucket.Versioning)
	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	}
	if bucket.Versioning == "Enabled" {
		result.VersionId = object.GetVersionId()
	}
	// update null version number
	if bucket.Versioning == "Suspended" {
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

//TODO: Append Support Encryption
func (yig *YigStorage) AppendObject(bucketName string, objectName string, credential common.Credential,
	offset uint64, size int64, data io.Reader, metadata map[string]string, acl datatype.Acl,
	sseRequest datatype.SseRequest, storageClass meta.StorageClass, objInfo *meta.Object) (result datatype.AppendObjectResult, err error) {

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

	var cephCluster *CephStorage
	var poolName, oid string
	var initializationVector []byte
	var objSize int64
	if isObjectExist(objInfo) {
		cephCluster, err = yig.GetClusterByFsName(objInfo.Location)
		if err != nil {
			return
		}
		// Every appendable file must be treated as a big file
		poolName = BIG_FILE_POOLNAME
		oid = objInfo.ObjectId
		initializationVector = objInfo.InitializationVector
		objSize = objInfo.Size
		storageClass = objInfo.StorageClass
		helper.Logger.Println(20, "request append oid:", oid, "iv:", initializationVector, "size:", objSize)
	} else {
		// New appendable object
		cephCluster, poolName = yig.PickOneClusterAndPool(bucketName, objectName, size, true)
		if cephCluster == nil || poolName != BIG_FILE_POOLNAME {
			helper.Debugln("PickOneClusterAndPool error")
			return result, ErrInternalError
		}
		// Mapping a shorter name for the object
		oid = cephCluster.GetUniqUploadName()
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
	bytesWritten, err := cephCluster.Append(poolName, oid, storageReader, offset, isObjectExist(objInfo))
	if err != nil {
		helper.Debugln("cephCluster.Append err:", err, poolName, oid, offset)
		return
	}

	if bytesWritten < size {
		return result, ErrIncompleteBody
	}

	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	if userMd5, ok := metadata["md5Sum"]; ok {
		if userMd5 != "" && userMd5 != calculatedMd5 {
			return result, ErrBadDigest
		}
	}

	result.Md5 = calculatedMd5

	if signVerifyReader, ok := data.(*signature.SignVerifyReader); ok {
		credential, err = signVerifyReader.Verify()
		if err != nil {
			return
		}
	}

	// TODO validate bucket policy and fancy ACL
	object := &meta.Object{
		Name:                 objectName,
		BucketName:           bucketName,
		Location:             cephCluster.Name,
		Pool:                 poolName,
		OwnerId:              credential.UserId,
		Size:                 objSize + bytesWritten,
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
	}

	result.LastModified = object.LastModifiedTime
	result.NextPosition = object.Size
	helper.Logger.Println(20, "Append info.", "bucket:", bucketName, "objName:", objectName, "oid:", oid,
		"objSize:", object.Size, "bytesWritten:", bytesWritten, "storageClass:", storageClass)
	err = yig.MetaStorage.AppendObject(object, isObjectExist(objInfo))
	if err != nil {
		return
	}

	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.ObjectTable, bucketName+":"+objectName+":")
		yig.DataCache.Remove(bucketName + ":" + objectName + ":" + object.GetVersionId())
	}
	return result, nil
}

func (yig *YigStorage) UpdateObjectAttrs(targetObject *meta.Object, credential common.Credential) (result datatype.PutObjectResult, err error) {

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

	err = yig.MetaStorage.UpdateObjectAttrs(targetObject)
	if err != nil {
		yig.Logger.Println(5, "Update Object Attrs, sql fails")
		return result, ErrInternalError
	}
	result.LastModified = targetObject.LastModifiedTime
	result.Md5 = targetObject.Etag
	result.VersionId = targetObject.GetVersionId()

	yig.MetaStorage.Cache.Remove(redis.ObjectTable, targetObject.BucketName+":"+targetObject.Name+":")
	yig.DataCache.Remove(targetObject.BucketName + ":" + targetObject.Name + ":" + targetObject.GetVersionId())

	return result, nil
}

func (yig *YigStorage) CopyObject(targetObject *meta.Object, source io.Reader, credential common.Credential,
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

	cephCluster, poolName := yig.PickOneClusterAndPool(targetObject.BucketName,
		targetObject.Name, targetObject.Size, false)

	if len(targetObject.Parts) != 0 {
		var targetParts map[int]*meta.Part = make(map[int]*meta.Part, len(targetObject.Parts))
		//		etaglist := make([]string, len(sourceObject.Parts))
		for partNum, part := range targetObject.Parts {
			targetParts[partNum] = part
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
			oid = cephCluster.GetUniqUploadName()
			var bytesW int64
			var storageReader io.Reader
			var initializationVector []byte
			if len(encryptionKey) != 0 {
				initializationVector, err = newInitializationVector()
				if err != nil {
					return
				}
			}
			storageReader, err = wrapEncryptionReader(dataReader, encryptionKey, initializationVector)
			bytesW, err = cephCluster.Put(poolName, oid, storageReader)
			maybeObjectToRecycle = objectToRecycle{
				location: cephCluster.Name,
				pool:     poolName,
				objectId: oid,
			}
			if bytesW < part.Size {
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
				return
			}
			part.LastModified = time.Now().UTC().Format(meta.CREATE_TIME_LAYOUT)
			part.ObjectId = oid

			part.InitializationVector = initializationVector
		}
		targetObject.ObjectId = ""
		targetObject.Parts = targetParts
		result.Md5 = targetObject.Etag
	} else {
		md5Writer := md5.New()

		// Mapping a shorter name for the object
		oid = cephCluster.GetUniqUploadName()
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
		var bytesWritten int64
		bytesWritten, err = cephCluster.Put(poolName, oid, storageReader)
		if err != nil {
			return
		}
		// Should metadata update failed, add `maybeObjectToRecycle` to `RecycleQueue`,
		// so the object in Ceph could be removed asynchronously
		maybeObjectToRecycle = objectToRecycle{
			location: cephCluster.Name,
			pool:     poolName,
			objectId: oid,
		}
		if bytesWritten < targetObject.Size {
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
	targetObject.Location = cephCluster.Name
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
	nullVerNum, err = yig.checkOldObject(targetObject.BucketName, targetObject.Name, bucket.Versioning)
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

	err = yig.MetaStorage.DeleteObject(object, object.DeleteMarker, objMap)
	if err != nil {
		return
	}
	return nil
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

func (yig *YigStorage) removeAllObjectsEntryByName(bucketName, objectName string) (err error) {

	objs, err := yig.MetaStorage.GetAllObject(bucketName, objectName)
	if err == ErrNoSuchKey {
		return nil
	}
	if err != nil {
		return err
	}
	for _, obj := range objs {
		err = yig.removeByObject(obj, nil)
		if err != nil {
			return err
		}
	}
	return
}

func (yig *YigStorage) checkOldObject(bucketName, objectName, versioning string) (version uint64, err error) {

	if versioning == "Disabled" {
		err = yig.removeAllObjectsEntryByName(bucketName, objectName)
		return
	}

	if versioning == "Enabled" || versioning == "Suspended" {
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
				/*decrypted, err := meta.Decrypt(object.GetVersionNumber())
				if err != nil {
					return []byte{}, err
				}
				version, err := strconv.ParseUint(decrypted, 10, 64)
				if err != nil {
					return []byte{}, ErrInvalidVersioning
				}*/
				version, err = object.GetVersionNumber()
				if err != nil {
					helper.Debugln("-----------old object version:", err)
					return 0, err
				}
				helper.Debugln("-----------old object version:", version)
				return
			}
		} else {
			helper.Debugln("object.NullVersion:", object.NullVersion)
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
func (yig *YigStorage) DeleteObject(bucketName string, objectName string, version string,
	credential common.Credential) (result datatype.DeleteObjectResult, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		return
	}
	switch bucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.OwnerId != credential.UserId && credential.UserId != "" {
			return result, ErrBucketAccessForbidden
		}
	} // TODO policy and fancy ACL

	switch bucket.Versioning {
	case "Disabled":
		if version != "" && version != "null" {
			return result, ErrNoSuchVersion
		}
		err = yig.removeAllObjectsEntryByName(bucketName, objectName)
		if err != nil {
			return
		}
	case "Enabled":
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
	case "Suspended":
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
		yig.Logger.Println(5, "Invalid bucket versioning: ", bucketName)
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

func isObjectExist(obj *meta.Object) bool {
	return obj != nil
}
