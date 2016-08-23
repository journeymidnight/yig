package storage

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/iam"
	"git.letv.cn/yig/yig/meta"
	"git.letv.cn/yig/yig/signature"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
	"io"
	"time"
	"git.letv.cn/yig/yig/helper"
)

func (yig *YigStorage) PickOneClusterAndPool(bucket string, object string, size int64) (cluster *CephStorage, poolName string) {
	// always choose the first cluster for testing
	if size < 0 { // request.ContentLength is -1 if length is unknown
		return yig.DataStorage["2fc32752-04a3-48dc-8297-40fb4dd11ff5"], BIG_FILE_POOLNAME
	}
	if size < BIG_FILE_THRESHOLD {
		return yig.DataStorage["2fc32752-04a3-48dc-8297-40fb4dd11ff5"], SMALL_FILE_POOLNAME
	} else {
		return yig.DataStorage["2fc32752-04a3-48dc-8297-40fb4dd11ff5"], BIG_FILE_POOLNAME
	}
}

func (yig *YigStorage) GetObject(object meta.Object, startOffset int64,
	length int64, writer io.Writer) (err error) {
	if len(object.Parts) == 0 { // this object has only one part
		cephCluster, ok := yig.DataStorage[object.Location]
		if !ok {
			return errors.New("Cannot find specified ceph cluster: " + object.Location)
		}
		err = cephCluster.get(object.Pool, object.ObjectId, startOffset, length, writer)
		return
	}
	// multipart uploaded object
	for i := 1; i <= len(object.Parts); i++ {
		p := object.Parts[i]
		if p.Offset > startOffset+length {
			return
		}
		if p.Offset+p.Size >= startOffset {
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
			cephCluster, ok := yig.DataStorage[p.Location]
			if !ok {
				return errors.New("Cannot find specified ceph cluster: " +
					p.Location)
			}
			err = cephCluster.get(p.Pool, p.ObjectId, readOffset, readLength, writer)
			if err != nil {
				return
			}
		}
	}
	return
}

func (yig *YigStorage) GetObjectInfo(bucketName string, objectName string,
version string) (meta.Object, error) {
	return yig.MetaStorage.GetObject(bucketName, objectName, version)
}

func (yig *YigStorage) SetObjectAcl(bucketName string, objectName string, acl datatype.Acl,
	credential iam.Credential) error {
	bucket, err := yig.MetaStorage.GetBucket(bucketName)
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
	object, err := yig.MetaStorage.GetObject(bucketName, objectName)
	if err != nil {
		return err
	}
	object.ACL = acl
	rowkey, err := object.GetRowkey()
	if err != nil {
		return err
	}
	values, err := object.GetValues()
	if err != nil {
		return err
	}
	put, err := hrpc.NewPutStr(context.Background(), meta.OBJECT_TABLE, rowkey, values)
	if err != nil {
		return err
	}
	_, err = yig.MetaStorage.Hbase.Put(put)
	if err != nil {
		return err
	}
	return nil
}

func (yig *YigStorage) PutObject(bucketName string, objectName string, size int64, data io.Reader,
metadata map[string]string, acl datatype.Acl) (result datatype.PutObjectResult, err error) {

	md5Writer := md5.New()

	// Limit the reader to its provided size if specified.
	var limitedDataReader io.Reader
	if size > 0 { // request.ContentLength is -1 if length is unknown
		limitedDataReader = io.LimitReader(data, size)
	} else {
		limitedDataReader = data
	}
	cephCluster, poolName := yig.PickOneClusterAndPool(bucketName, objectName, size)

	// Mapping a shorter name for the object
	oid := cephCluster.GetUniqUploadName()
	storageReader := io.TeeReader(limitedDataReader, md5Writer)
	bytesWritten, err := cephCluster.put(poolName, oid, storageReader)
	if err != nil {
		return
	}
	if bytesWritten < size {
		return result, ErrIncompleteBody
	}

	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	if userMd5, ok := metadata["md5Sum"]; ok {
		if userMd5 != calculatedMd5 {
			return result, ErrBadDigest
		}
	}
	result.Md5 = calculatedMd5

	credential, err := data.(*signature.SignVerifyReader).Verify()
	if err != nil {
		return
	}

	bucket, err := yig.MetaStorage.GetBucket(bucketName)
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
	// TODO validate bucket policy and fancy ACL

	object := meta.Object{
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
		NullVersion:	  helper.Ternary(bucket.Versioning == "Enabled", false, true),
		DeleteMarker:	  false,
		// TODO CustomAttributes
	}

	var olderObject meta.Object
	if bucket.Versioning == "Enabled" {
		result.VersionId = object.GetVersionId()
	} else { // remove older object if versioning is not enabled
		olderObject, err = yig.MetaStorage.GetObject(bucketName, objectName, "")
		if err != nil {
			return
		}
		if olderObject.NullVersion {
			err = deleteObjectEntry(olderObject, yig.MetaStorage)
			if err != nil {
				return
			}
			err = putObjectToGarbageCollection(olderObject, yig.MetaStorage)
			if err != nil { // try to rollback `objects` table
				yig.Logger.Println("Error putObjectToGarbageCollection: ", err)
				err = insertObjectEntry(olderObject, yig.MetaStorage)
				if err != nil {
					yig.Logger.Println("Error insertObjectEntry: ", err)
					yig.Logger.Println("Inconsistent data: object should be removed:",
						olderObject)
					return
				}
				return result, ErrInternalError
			}
		}
	}

	err = insertObjectEntry(object, yig.MetaStorage)
	if err != nil {
		return
	}
	return result, nil
}

func insertObjectEntry(object meta.Object, metaStorage *meta.Meta) error {
	rowkey, err := object.GetRowkey()
	if err != nil {
		return "", err
	}
	values, err := object.GetValues()
	if err != nil {
		return "", err
	}
	put, err := hrpc.NewPutStr(context.Background(), meta.OBJECT_TABLE,
		rowkey, values)
	if err != nil {
		return "", err
	}
	_, err = metaStorage.Hbase.Put(put)
	return err
}

func deleteObjectEntry(object meta.Object, metaStorage *meta.Meta) error {
	rowkeyToDelete, err := object.GetRowkey()
	if err != nil {
		return err
	}
	deleteRequest, err := hrpc.NewDelStr(context.Background(), meta.OBJECT_TABLE,
		rowkeyToDelete, object.GetValuesForDelete())
	if err != nil {
		return err
	}
	_, err = metaStorage.Hbase.Delete(deleteRequest)
	return err
}

// Insert object to `garbageCollection` table
func putObjectToGarbageCollection(object meta.Object, metaStorage *meta.Meta) error {
	garbageCollection := meta.GarbageCollectionFromObject(object)

	garbageCollectionValues, err := garbageCollection.GetValues()
	if err != nil {
		return err
	}
	garbageCollectionRowkey, err := garbageCollection.GetRowkey()
	if err != nil {
		return err
	}
	putRequest, err := hrpc.NewPutStr(context.Background(), meta.GARBAGE_COLLECTION_TABLE,
		garbageCollectionRowkey, garbageCollectionValues)
	if err != nil {
		return err
	}
	_, err = metaStorage.Hbase.Put(putRequest)
	return err
}

func (yig *YigStorage) DeleteObject(bucketName string, objectName string) error {
	// TODO validate policy and ACL
	// TODO versioning
	object, err := yig.MetaStorage.GetObject(bucketName, objectName)
	if err != nil {
		return err
	}

	err = deleteObjectEntry(object, yig.MetaStorage)
	if err != nil {
		return err
	}

	err = putObjectToGarbageCollection(object, yig.MetaStorage)
	if err != nil {  // try to rollback `objects` table
		yig.Logger.Println("Error putObjectToGarbageCollection: ", err)
		err = insertObjectEntry(object, yig.MetaStorage)
		if err != nil {
			yig.Logger.Println("Error insertObjectEntry: ", err)
			yig.Logger.Println("Inconsistent data: object should be removed:",
				object)
			return err
		}
		return ErrInternalError
	}
	// TODO a daemon to check garbage collection table and delete objects in ceph
	return nil
}
