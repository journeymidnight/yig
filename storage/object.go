package storage

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"git.letv.cn/yig/yig/meta"
	"git.letv.cn/yig/yig/signature"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
	"io"
	"time"
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
	cephCluster, ok := yig.DataStorage[object.Location]
	if !ok {
		return errors.New("Cannot find specified ceph cluster: " + object.Location)
	}
	err = cephCluster.get(object.Pool, object.ObjectId, startOffset, length, writer)
	return
}

func (yig *YigStorage) GetObjectInfo(bucketName string, objectName string) (meta.Object, error) {
	return yig.MetaStorage.GetObject(bucketName, objectName)
}

func (yig *YigStorage) PutObject(bucketName string, objectName string, size int64,
	data io.Reader, metadata map[string]string) (md5String string, err error) {
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
		return "", err
	}
	if bytesWritten < size {
		return "", meta.IncompleteBody{
			Bucket: bucketName,
			Object: objectName,
		}
	}

	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	if userMd5, ok := metadata["md5Sum"]; ok {
		if userMd5 != calculatedMd5 {
			return "", meta.BadDigest{
				ExpectedMD5:   userMd5,
				CalculatedMD5: calculatedMd5,
			}
		}
	}

	credential, err := data.(*signature.SignVerifyReader).Verify()
	if err != nil {
		return "", err
	}

	bucket, err := yig.MetaStorage.GetBucketInfo(bucketName)
	if err != nil {
		return "", err
	}

	if bucket.OwnerId != credential.UserId {
		return "", meta.BucketAccessForbidden{Bucket: bucketName}
		// TODO validate bucket policy and ACL
	}

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
		// TODO CustomAttributes
	}

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
	_, err = yig.MetaStorage.Hbase.Put(put)
	if err != nil {
		return "", err
	}
	// TODO: remove old object of same name
	// TODO: versioning
	return calculatedMd5, nil
}

func (yig *YigStorage) DeleteObject(bucketName string, objectName string) error {
	// TODO validate policy and ACL
	// TODO versioning
	object, err := yig.MetaStorage.GetObject(bucketName, objectName)
	if err != nil {
		return err
	}
	rowkeyToDelete, err := object.GetRowkey()
	if err != nil {
		return err
	}
	deleteRequest, err := hrpc.NewDelStr(context.Background(), meta.OBJECT_TABLE,
		rowkeyToDelete, object.GetValuesForDelete())
	if err != nil {
		return err
	}
	_, err = yig.MetaStorage.Hbase.Delete(deleteRequest)
	if err != nil {
		return err
	}

	garbageCollectionValues := map[string]map[string][]byte{
		meta.GARBAGE_COLLECTION_COLUMN_FAMILY: map[string][]byte{
			"location": []byte(object.Location),
			"pool":     []byte(object.Pool),
			"oid":      []byte(object.ObjectId),
		},
	}
	garbageCollectionRowkey, err := meta.GetGarbageCollectionRowkey(bucketName, objectName)
	if err != nil {
		return err
	}
	putRequest, err := hrpc.NewPutStr(context.Background(), meta.GARBAGE_COLLECTION_TABLE,
		garbageCollectionRowkey, garbageCollectionValues)
	if err != nil {
		return err
	}
	_, err = yig.MetaStorage.Hbase.Put(putRequest)
	if err != nil {
		yig.Logger.Println("Error making hbase put: ", err)
		yig.Logger.Println("Inconsistent data: object with oid ", object.ObjectId,
			"should be removed in ", object.Location, object.Pool)
		return err
	}
	// TODO a daemon to check garbage collection table and delete objects in ceph
	return nil
}
