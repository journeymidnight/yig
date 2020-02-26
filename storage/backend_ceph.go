package storage

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"sync"
	"time"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/backend"
	"github.com/journeymidnight/yig/ceph"
	"github.com/journeymidnight/yig/crypto"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/meta"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/signature"
)

func New(metaCacheType int, enableDataCache bool, kms crypto.KMS) *YigStorage {
	yig := YigStorage{
		DataStorage: make(map[string]backend.Cluster),
		DataCache:   newDataCache(enableDataCache),
		MetaStorage: meta.New(meta.CacheType(metaCacheType)),
		KMS:         kms,
		Stopping:    false,
		WaitGroup:   new(sync.WaitGroup),
	}

	yig.DataStorage = ceph.Initialize(helper.CONFIG)
	if len(yig.DataStorage) == 0 {
		panic("No data storage can be used!")
	}

	initializeRecycler(&yig)
	return &yig
}

//TODO: Append Support Encryption
func (yig *YigStorage) AppendObject(bucketName string, objectName string, credential common.Credential,
	offset uint64, size int64, data io.ReadCloser, metadata map[string]string, acl datatype.Acl,
	sseRequest datatype.SseRequest, storageClass types.StorageClass, objInfo *types.Object) (result datatype.AppendObjectResult, err error) {

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
		cephCluster, poolName = yig.pickClusterAndPool(bucketName, objectName, size, true)
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
	object := &types.Object{
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
		Type:                 types.ObjectTypeAppendable,
		StorageClass:         storageClass,
		VersionId:            "0",
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
		yig.MetaStorage.Cache.Remove(redis.ObjectTable, bucketName+":"+objectName+":")
		yig.DataCache.Remove(bucketName + ":" + objectName + ":" + object.GetVersionId())
	}
	return result, nil
}
