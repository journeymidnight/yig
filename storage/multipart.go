package storage

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/url"
	"strconv"
	"time"

	"github.com/journeymidnight/yig/api"
	"github.com/journeymidnight/yig/api/datatype"
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

const (
	MAX_PART_SIZE   = 5 << 30 // 5GB
	MAX_PART_NUMBER = 10000
)

func (yig *YigStorage) ListMultipartUploads(reqCtx RequestContext, credential common.Credential,
	request datatype.ListUploadsRequest) (result datatype.ListMultipartUploadsResponse, err error) {

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
	// TODO policy and fancy ACL

	result, err = yig.MetaStorage.Client.ListMultipartUploads(bucket.Name, request.KeyMarker, request.UploadIdMarker, request.Prefix, request.Delimiter, request.EncodingType, request.MaxUploads)
	if err != nil {
		return
	}

	result.EncodingType = request.EncodingType
	if result.EncodingType != "" { // only support "url" encoding for now
		result.Delimiter = url.QueryEscape(result.Delimiter)
		result.KeyMarker = url.QueryEscape(result.KeyMarker)
		result.Prefix = url.QueryEscape(result.Prefix)
		result.NextKeyMarker = url.QueryEscape(result.NextKeyMarker)
	}
	return
}

func (yig *YigStorage) NewMultipartUpload(reqCtx RequestContext, credential common.Credential,
	metadata map[string]string, acl datatype.Acl,
	sseRequest datatype.SseRequest, storageClass meta.StorageClass) (uploadId string, err error) {
	bucketName, objectName := reqCtx.BucketName, reqCtx.ObjectName
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return "", ErrNoSuchBucket
	}
	switch bucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.OwnerId != credential.UserId {
			return "", ErrBucketAccessForbidden
		}
	}
	// TODO policy and fancy ACL

	contentType, ok := metadata["Content-Type"]
	if !ok {
		contentType = "application/octet-stream"
	}

	cephCluster, pool := yig.pickClusterAndPool(bucketName, objectName, storageClass, -1, false)
	multipartMetadata := meta.MultipartMetadata{
		InitiatorId:  credential.UserId,
		OwnerId:      bucket.OwnerId,
		ContentType:  contentType,
		Location:     cephCluster.ID(),
		Pool:         pool,
		Acl:          acl,
		SseRequest:   sseRequest,
		Attrs:        metadata,
		StorageClass: storageClass,
	}
	if sseRequest.Type == crypto.S3.String() {
		multipartMetadata.EncryptionKey, multipartMetadata.CipherKey, err = yig.encryptionKeyFromSseRequest(sseRequest, bucketName, objectName)
		if err != nil {
			return
		}
	} else {
		multipartMetadata.EncryptionKey = nil
	}

	multipart := meta.Multipart{
		BucketName:  bucketName,
		ObjectName:  objectName,
		InitialTime: uint64(time.Now().UTC().UnixNano()),
		Metadata:    multipartMetadata,
	}

	err = multipart.GenUploadId()
	if err != nil {
		return
	}
	err = yig.MetaStorage.Client.CreateMultipart(multipart)
	if err != nil {
		return
	}
	return multipart.UploadId, nil
}

func (yig *YigStorage) PutObjectPart(reqCtx RequestContext, credential common.Credential,
	uploadId string, partId int, size int64, data io.ReadCloser, md5Hex string,
	sseRequest datatype.SseRequest) (result datatype.PutObjectPartResult, err error) {

	defer data.Close()
	bucketName, objectName := reqCtx.BucketName, reqCtx.ObjectName
	multipart, err := yig.MetaStorage.GetMultipart(bucketName, objectName, uploadId)
	if err != nil {
		return
	}

	if size > MAX_PART_SIZE {
		err = ErrEntityTooLarge
		return
	}

	var encryptionKey []byte
	switch multipart.Metadata.SseRequest.Type {
	case "":
		break
	case crypto.SSEC.String():
		if sseRequest.Type != crypto.SSEC.String() {
			err = ErrInvalidSseHeader
			return
		}
		encryptionKey = sseRequest.SseCustomerKey
	case crypto.S3.String():
		encryptionKey = multipart.Metadata.EncryptionKey
	case crypto.S3KMS.String():
		err = ErrNotImplemented
		return
	}

	md5Writer := md5.New()
	limitedDataReader := io.LimitReader(data, size)
	poolName := multipart.Metadata.Pool
	cluster, err := yig.GetClusterByFsName(multipart.Metadata.Location)
	if err != nil {
		return
	}
	dataReader := io.TeeReader(limitedDataReader, md5Writer)

	var initializationVector []byte
	if len(encryptionKey) != 0 {
		initializationVector, err = newInitializationVector()
		if err != nil {
			return
		}
	}
	storageReader, err := wrapEncryptionReader(dataReader, encryptionKey,
		initializationVector)
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
		err = ErrIncompleteBody
		return
	}

	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	if md5Hex != "" && md5Hex != calculatedMd5 {
		RecycleQueue <- maybeObjectToRecycle
		err = ErrBadDigest
		return
	}

	if signVerifyReader, ok := data.(*signature.SignVerifyReadCloser); ok {
		credential, err = signVerifyReader.Verify()
		if err != nil {
			RecycleQueue <- maybeObjectToRecycle
			return
		}
	}

	part := meta.Part{
		PartNumber:           partId,
		Size:                 size,
		ObjectId:             objectId,
		Etag:                 calculatedMd5,
		LastModified:         time.Now().UTC().Format(meta.CREATE_TIME_LAYOUT),
		InitializationVector: initializationVector,
	}
	err = yig.MetaStorage.PutObjectPart(multipart, part)
	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	}
	// remove possible old object in Ceph
	if part, ok := multipart.Parts[partId]; ok {
		RecycleQueue <- objectToRecycle{
			location: multipart.Metadata.Location,
			pool:     multipart.Metadata.Pool,
			objectId: part.ObjectId,
		}
	}

	result.ETag = calculatedMd5
	result.SseType = sseRequest.Type
	result.SseAwsKmsKeyIdBase64 = base64.StdEncoding.EncodeToString([]byte(sseRequest.SseAwsKmsKeyId))
	result.SseCustomerAlgorithm = sseRequest.SseCustomerAlgorithm
	result.SseCustomerKeyMd5Base64 = base64.StdEncoding.EncodeToString(sseRequest.SseCustomerKey)
	return result, nil
}

func (yig *YigStorage) CopyObjectPart(bucketName, objectName, uploadId string, partId int,
	size int64, data io.Reader, credential common.Credential,
	sseRequest datatype.SseRequest) (result datatype.PutObjectResult, err error) {

	multipart, err := yig.MetaStorage.GetMultipart(bucketName, objectName, uploadId)
	if err != nil {
		return
	}

	if size > MAX_PART_SIZE {
		err = ErrEntityTooLarge
		return
	}

	var encryptionKey []byte
	switch multipart.Metadata.SseRequest.Type {
	case "":
		break
	case crypto.SSEC.String():
		if sseRequest.Type != crypto.SSEC.String() {
			err = ErrInvalidSseHeader
			return
		}
		encryptionKey = sseRequest.SseCustomerKey
	case crypto.S3.String():
		encryptionKey = multipart.Metadata.EncryptionKey
	case crypto.S3KMS.String():
		err = ErrNotImplemented
		return
	}

	md5Writer := md5.New()
	limitedDataReader := io.LimitReader(data, size)
	poolName := multipart.Metadata.Pool
	cephCluster, err := yig.GetClusterByFsName(multipart.Metadata.Location)
	if err != nil {
		return
	}
	dataReader := io.TeeReader(limitedDataReader, md5Writer)

	var initializationVector []byte
	if len(encryptionKey) != 0 {
		initializationVector, err = newInitializationVector()
		if err != nil {
			return
		}
	}
	storageReader, err := wrapEncryptionReader(dataReader, encryptionKey,
		initializationVector)
	if err != nil {
		return
	}
	objectId, bytesWritten, err := cephCluster.Put(poolName, storageReader)
	if err != nil {
		return
	}
	// Should metadata update failed, add `maybeObjectToRecycle` to `RecycleQueue`,
	// so the object in Ceph could be removed asynchronously
	maybeObjectToRecycle := objectToRecycle{
		location: cephCluster.ID(),
		pool:     poolName,
		objectId: objectId,
	}

	if int64(bytesWritten) < size {
		RecycleQueue <- maybeObjectToRecycle
		err = ErrIncompleteBody
		return
	}

	result.Md5 = hex.EncodeToString(md5Writer.Sum(nil))

	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	}
	switch bucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.OwnerId != credential.UserId {
			RecycleQueue <- maybeObjectToRecycle
			err = ErrBucketAccessForbidden
			return
		}
	} // TODO policy and fancy ACL

	if initializationVector == nil {
		initializationVector = []byte{}
	}
	now := time.Now().UTC()
	part := meta.Part{
		PartNumber:           partId,
		Size:                 size,
		ObjectId:             objectId,
		Etag:                 result.Md5,
		LastModified:         now.Format(meta.CREATE_TIME_LAYOUT),
		InitializationVector: initializationVector,
	}
	result.LastModified = now

	err = yig.MetaStorage.PutObjectPart(multipart, part)
	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	}

	// remove possible old object in Ceph
	if part, ok := multipart.Parts[partId]; ok {
		RecycleQueue <- objectToRecycle{
			location: multipart.Metadata.Location,
			pool:     multipart.Metadata.Pool,
			objectId: part.ObjectId,
		}
	}

	return result, nil
}

func (yig *YigStorage) ListObjectParts(credential common.Credential, bucketName, objectName string,
	request datatype.ListPartsRequest) (result datatype.ListPartsResponse, err error) {

	multipart, err := yig.MetaStorage.GetMultipart(bucketName, objectName, request.UploadId)
	if err != nil {
		return
	}

	initiatorId := multipart.Metadata.InitiatorId
	ownerId := multipart.Metadata.OwnerId

	switch multipart.Metadata.Acl.CannedAcl {
	case "public-read", "public-read-write":
		break
	case "authenticated-read":
		if credential.UserId == "" {
			err = ErrAccessDenied
			return
		}
	case "bucket-owner-read", "bucket-owner-full-controll":
		var bucket *meta.Bucket
		bucket, err = yig.MetaStorage.GetBucket(bucketName, true)
		if err != nil {
			return
		}
		if bucket.OwnerId != credential.UserId {
			err = ErrAccessDenied
			return
		}
	default:
		if ownerId != credential.UserId {
			err = ErrAccessDenied
			return
		}
	}
	for i := request.PartNumberMarker + 1; i <= MAX_PART_NUMBER; i++ {
		if p, ok := multipart.Parts[i]; ok {
			part := datatype.Part{
				PartNumber:   i,
				ETag:         "\"" + p.Etag + "\"",
				LastModified: p.LastModified,
				Size:         p.Size,
			}
			result.Parts = append(result.Parts, part)

			if len(result.Parts) > request.MaxParts {
				break
			}
		}
	}
	if len(result.Parts) == request.MaxParts+1 {
		result.IsTruncated = true
		result.NextPartNumberMarker = result.Parts[request.MaxParts].PartNumber
		result.Parts = result.Parts[:request.MaxParts]
	}

	var user common.Credential
	user, err = iam.GetCredentialByUserId(ownerId)
	if err != nil {
		return
	}
	result.Owner.ID = user.UserId
	result.Owner.DisplayName = user.DisplayName
	user, err = iam.GetCredentialByUserId(initiatorId)
	if err != nil {
		return
	}
	result.Initiator.ID = user.UserId
	result.Initiator.DisplayName = user.DisplayName

	result.Bucket = bucketName
	result.Key = objectName
	result.UploadId = request.UploadId
	result.StorageClass = multipart.Metadata.StorageClass.ToString()
	result.PartNumberMarker = request.PartNumberMarker
	result.MaxParts = request.MaxParts
	result.EncodingType = request.EncodingType

	if result.EncodingType != "" { // only support "url" encoding for now
		result.Key = url.QueryEscape(result.Key)
	}
	return
}

func (yig *YigStorage) AbortMultipartUpload(reqCtx RequestContext, credential common.Credential, uploadId string) error {
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return ErrNoSuchBucket
	}
	switch bucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.OwnerId != credential.UserId {
			return ErrBucketAccessForbidden
		}
	} // TODO policy and fancy ACL

	bucketName, objectName := reqCtx.BucketName, reqCtx.ObjectName
	multipart, err := yig.MetaStorage.GetMultipart(bucketName, objectName, uploadId)
	if err != nil {
		return err
	}

	err = yig.MetaStorage.DeleteMultipart(multipart)
	if err != nil {
		return err
	}
	// remove parts in Ceph

	for _, p := range multipart.Parts {
		RecycleQueue <- objectToRecycle{
			location: multipart.Metadata.Location,
			pool:     multipart.Metadata.Pool,
			objectId: p.ObjectId,
		}
	}

	return nil
}

func (yig *YigStorage) CompleteMultipartUpload(reqCtx RequestContext, credential common.Credential, uploadId string, uploadedParts []meta.CompletePart) (result datatype.CompleteMultipartResult,
	err error) {
	bucketName, objectName := reqCtx.BucketName, reqCtx.ObjectName
	bucket := reqCtx.BucketInfo
	if bucket == nil {
		return result, ErrNoSuchBucket
	}
	switch bucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.OwnerId != credential.UserId {
			err = ErrBucketAccessForbidden
			return
		}
	}
	// TODO policy and fancy ACL

	multipart, err := yig.MetaStorage.GetMultipart(bucketName, objectName, uploadId)
	if err != nil {
		return
	}

	md5Writer := md5.New()
	var totalSize int64 = 0
	helper.Logger.Info("Upload parts:", uploadedParts, "uploadId:", uploadId)
	for i := 0; i < len(uploadedParts); i++ {
		if uploadedParts[i].PartNumber != i+1 {
			helper.Logger.Error("uploadedParts[i].PartNumber != i+1; i:", i,
				"uploadId:", uploadId)
			err = ErrInvalidPart
			return
		}
		part, ok := multipart.Parts[i+1]
		if !ok {
			helper.Logger.Error("multipart.Parts[i+1] does not exist; i:", i,
				"uploadId:", uploadId)
			err = ErrInvalidPart
			return
		}
		if part.Size < api.MIN_PART_SIZE && part.PartNumber != len(uploadedParts) {
			err = meta.PartTooSmall{
				PartSize:   part.Size,
				PartNumber: part.PartNumber,
				PartETag:   part.Etag,
			}
			return
		}
		if part.Etag != uploadedParts[i].ETag {
			helper.Logger.Error("part.Etag != uploadedParts[i].ETag;",
				"i:", i, "Etag:", part.Etag, "reqEtag:",
				uploadedParts[i].ETag, "uploadId:", uploadId)
			err = ErrInvalidPart
			return
		}
		var etagBytes []byte
		etagBytes, err = hex.DecodeString(part.Etag)
		if err != nil {
			helper.Logger.Error("hex.DecodeString(part.Etag) err:", err,
				"uploadId:", uploadId)
			err = ErrInvalidPart
			return
		}
		part.Offset = totalSize
		totalSize += part.Size
		md5Writer.Write(etagBytes)
	}
	result.ETag = hex.EncodeToString(md5Writer.Sum(nil))
	result.ETag += "-" + strconv.Itoa(len(uploadedParts))
	// See http://stackoverflow.com/questions/12186993
	// for how to calculate multipart Etag

	// Add to objects table
	contentType := multipart.Metadata.ContentType
	object := &meta.Object{
		Name:             objectName,
		BucketName:       bucketName,
		OwnerId:          multipart.Metadata.OwnerId,
		Pool:             multipart.Metadata.Pool,
		Location:         multipart.Metadata.Location,
		Size:             totalSize,
		LastModifiedTime: time.Now().UTC(),
		Etag:             result.ETag,
		ContentType:      contentType,
		Parts:            multipart.Parts,
		ACL:              multipart.Metadata.Acl,
		NullVersion:      helper.Ternary(bucket.Versioning == datatype.BucketVersioningEnabled, false, true).(bool),
		DeleteMarker:     false,
		SseType:          multipart.Metadata.SseRequest.Type,
		EncryptionKey:    multipart.Metadata.CipherKey,
		CustomAttributes: multipart.Metadata.Attrs,
		Type:             meta.ObjectTypeMultipart,
		StorageClass:     multipart.Metadata.StorageClass,
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

	err = yig.MetaStorage.PutObject(reqCtx, object, &multipart, false)
	if err != nil {
		return
	}

	sseRequest := multipart.Metadata.SseRequest
	result.SseType = sseRequest.Type
	result.SseAwsKmsKeyIdBase64 = base64.StdEncoding.EncodeToString([]byte(sseRequest.SseAwsKmsKeyId))
	result.SseCustomerAlgorithm = sseRequest.SseCustomerAlgorithm
	result.SseCustomerKeyMd5Base64 = base64.StdEncoding.EncodeToString(sseRequest.SseCustomerKey)

	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.ObjectTable, bucketName+":"+objectName+":"+object.VersionId)
		yig.DataCache.Remove(bucketName + ":" + objectName + ":" + object.VersionId)
	}

	return
}
