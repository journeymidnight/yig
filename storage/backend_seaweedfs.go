// +build seaweedfs

package storage

import (
	"crypto/md5"
	"encoding/hex"
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/backend"
	"github.com/journeymidnight/yig/crypto"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/seaweed"
	"github.com/journeymidnight/yig/signature"
	"io"
	"sync"
	"time"
)

func New(logger *log.Logger, metaCacheType int, enableDataCache bool) *YigStorage {
	kms := crypto.NewKMS()
	yig := YigStorage{
		DataStorage: make(map[string]backend.Cluster),
		DataCache:   newDataCache(enableDataCache),
		MetaStorage: meta.New(logger, meta.CacheType(metaCacheType)),
		KMS:         kms,
		Logger:      logger,
		Stopping:    false,
		WaitGroup:   new(sync.WaitGroup),
	}

	yig.DataStorage = seaweed.Initialize(logger, helper.CONFIG)
	if len(yig.DataStorage) == 0 {
		helper.Logger.Panic(0, "PANIC: No data storage can be used!")
	}

	initializeRecycler(&yig)
	return &yig
}

func (yig *YigStorage) pickClusterAndPool(bucket string, object string,
	size int64, isAppend bool) (cluster backend.Cluster, poolName string) {
	// TODO cluster picking logic
	cluster, poolName, _ = seaweed.PickCluster(yig.DataStorage,
		nil, uint64(size), 0, 0)
	return cluster, poolName
}

// TODO: Append Support Encryption
// TODO: Support version
// FIXME: ETag calculation
func (yig *YigStorage) AppendObject(bucketName string, objectName string,
	credential common.Credential, offset uint64, size int64, data io.Reader,
	metadata map[string]string, acl datatype.Acl, sseRequest datatype.SseRequest,
	storageClass types.StorageClass,
	objectMeta *types.Object) (result datatype.AppendObjectResult, err error) {

	var cluster backend.Cluster
	var poolName string
	var initializationVector []byte
	now := time.Now().UTC()

	objectMetaExists := true
	if objectMeta == nil {
		objectMetaExists = false
		// New appendable object
		cluster, poolName = yig.pickClusterAndPool(
			bucketName, objectName, size, true)
		// FIXME poolName
		if cluster == nil || poolName != "" {
			helper.Debugln("pickClusterAndPool error")
			return result, ErrInternalError
		}
		objectMeta = &types.Object{
			Name:                 objectName,
			BucketName:           bucketName,
			Location:             cluster.ID(),
			Pool:                 poolName,
			OwnerId:              credential.UserId,
			Size:                 0, // updated below
			ObjectId:             "",
			LastModifiedTime:     now,
			Etag:                 "-", // FIXME workaround, s3cmd would ignore ETags with "-"
			ContentType:          metadata["Content-Type"],
			ACL:                  acl,
			NullVersion:          true,
			DeleteMarker:         false,
			SseType:              sseRequest.Type,
			EncryptionKey:        []byte(""),
			InitializationVector: initializationVector,
			CustomAttributes:     metadata,
			Parts:                make(map[int]*types.Part), // updated below
			Type:                 types.ObjectTypeAppendable,
			StorageClass:         storageClass,
		}
	}
	cluster, err = yig.GetClusterByFsName(objectMeta.Location)
	if err != nil {
		helper.Debugln("GetClusterByFsName error", err)
		return result, ErrInternalError
	}
	poolName = objectMeta.Pool
	md5Writer := md5.New()
	dataReader := io.TeeReader(data, md5Writer)
	// FIXME: assume append size < ObjectSizeLimit(30M) here
	var partToUpdate, partToCreate *types.Part
	var updatePartReader, createPartReader io.Reader
	var updatePartOffset int64
	partNumber := (offset / seaweed.ObjectSizeLimit) + 1
	if objectMeta.Parts[int(partNumber)] == nil {
		partToCreate = &types.Part{
			PartNumber:           int(partNumber),
			Size:                 size,
			ObjectId:             "",
			Offset:               int64(partNumber-1) * seaweed.ObjectSizeLimit,
			Etag:                 "",
			LastModified:         now.Format(types.CREATE_TIME_LAYOUT),
			InitializationVector: initializationVector,
		}
		createPartReader = io.LimitReader(dataReader, size)
	} else {
		partToUpdate = objectMeta.Parts[int(partNumber)]
		partToUpdate.LastModified = now.Format(types.CREATE_TIME_LAYOUT)
		updatePartOffset = partToUpdate.Size
		if partToUpdate.Size+size > seaweed.ObjectSizeLimit {
			updatePartReader = io.LimitReader(dataReader,
				seaweed.ObjectSizeLimit-partToUpdate.Size)
			partToUpdate.Size = seaweed.ObjectSizeLimit

			partToCreate = &types.Part{
				PartNumber:           partToUpdate.PartNumber + 1,
				Size:                 partToUpdate.Size + size - seaweed.ObjectSizeLimit,
				Offset:               int64(partToUpdate.PartNumber) * seaweed.ObjectSizeLimit,
				Etag:                 "",
				LastModified:         now.Format(types.CREATE_TIME_LAYOUT),
				InitializationVector: initializationVector,
			}
			createPartReader = io.LimitReader(dataReader, partToCreate.Size)
		} else {
			partToUpdate.Size += size
			updatePartReader = io.LimitReader(dataReader, size)
		}
	}

	var bytesWritten, n uint64
	if partToUpdate != nil {
		_, n, err = cluster.Append(poolName, partToUpdate.ObjectId,
			updatePartReader, updatePartOffset)
		if err != nil {
			helper.Debugln("cluster.Append err:", err,
				poolName, partToUpdate.ObjectId, offset)
			return
		}
		bytesWritten += n
		err = yig.MetaStorage.AppendObjectPart(bucketName, objectName, "",
			partToUpdate)
		if err != nil {
			helper.Debugln("AppendObjectPart err:", err)
			return
		}
	}
	if partToCreate != nil {
		var objectId string
		objectId, n, err = cluster.Append(poolName, "",
			createPartReader, 0)
		if err != nil {
			helper.Debugln("cluster.Append err:", err,
				poolName, partToCreate.ObjectId, offset)
			return
		}
		partToCreate.ObjectId = objectId
		bytesWritten += n
		err = yig.MetaStorage.CreateObjectPart(bucketName, objectName, "",
			partToCreate)
		if err != nil {
			helper.Debugln("CreateObjectPart err:", err)
			return
		}
	}

	if bytesWritten < uint64(size) {
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

	result.LastModified = now
	result.NextPosition = int64(offset) + size
	objectMeta.Size += size
	objectMeta.LastModifiedTime = now
	helper.Logger.Println(20, "Append info.", "bucket:", bucketName,
		"objName:", objectName, "parts:", objectMeta.Parts, "objSize:", size,
		"bytesWritten:", bytesWritten, "storageClass:", storageClass)
	// Note we don't update objectMeta.Parts in current method,
	// so yig.MetaStorage.AppendObject won't touch those entries in db.
	err = yig.MetaStorage.AppendObject(objectMeta, objectMetaExists)
	if err != nil {
		return
	}

	yig.MetaStorage.Cache.Remove(redis.ObjectTable, bucketName+":"+objectName+":")
	yig.DataCache.Remove(bucketName + ":" + objectName + ":" + objectMeta.GetVersionId())
	return result, nil
}
