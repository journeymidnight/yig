package storage

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"git.letv.cn/ceph/radoshttpd/rados"
	"git.letv.cn/yig/yig/meta"
	"git.letv.cn/yig/yig/minio"
	"git.letv.cn/yig/yig/minio/datatype"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
	"io"
	"time"
)

func (yig *YigStorage) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	return
}

func (yig *YigStorage) GetObjectInfo(bucket, object string) (objInfo datatype.ObjectInfo, err error) {
	return
}

func getWriter(objectName string, pool *rados.Pool) (io.Writer, error) {
	ioContext, err := pool.CreateStriper()

	if err != nil {
		return nil, err
	}
	if ret := ioContext.SetLayoutStripeUnit(STRIPE_UNIT); ret < 0 {
		return nil, errors.New("Cannot set stripe unit")
	}
	if ret := ioContext.SetLayoutObjectSize(OBJECT_SIZE); ret < 0 {
		return nil, errors.New("Cannot set object size")
	}
	if ret := ioContext.SetLayoutStripeCount(STRIPE_COUNT); ret < 0 {
		return nil, errors.New("cannot set stripe count")
	}

	return IoContextWrapper{
		oid:     objectName,
		striper: &ioContext,
		offset:  0,
	}, nil
}

func getMappedObjectName(bucketName string, objectName string) string {
	return bucketName + objectName
}

func (yig *YigStorage) PutObject(bucketName string, objectName string, size int64,
	data io.Reader, metadata map[string]string) (md5String string, err error) {
	md5Writer := md5.New()

	// Limit the reader to its provided size if specified.
	var limitedDataReader io.Reader
	var pool *rados.Pool
	var poolName string
	var bufferSize int
	if size > 0 { // request.ContentLength is -1 if length is unknown
		limitedDataReader = io.LimitReader(data, size)
		if size > BIG_FILE_THRESHOLD {
			pool = yig.DataStorage.BigFilePool
			poolName = BIG_FILE_POOL_NAME
		} else {
			pool = yig.DataStorage.SmallFilePool
			poolName = SMALL_FILE_POOL_NAME
		}
		if size > BUFFER_SIZE {
			bufferSize = BUFFER_SIZE
		} else {
			bufferSize = size
		}
	} else {
		limitedDataReader = data
		pool = yig.DataStorage.BigFilePool
		poolName = BIG_FILE_POOL_NAME
		bufferSize = BUFFER_SIZE
	}

	mappedObjectName := getMappedObjectName(bucketName, objectName)
	storageWriter, err := getWriter(mappedObjectName, pool)

	buffer := make([]byte, bufferSize)
	storageReader := io.TeeReader(limitedDataReader, md5Writer)
	bytesWritten, err := io.CopyBuffer(storageWriter, storageReader, buffer)
	if err != nil {
		return "", err
	}
	if bytesWritten < size {
		return "", datatype.IncompleteBody{
			Bucket: bucketName,
			Object: objectName,
		}
	}

	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	if userMd5, ok := metadata["md5Sum"]; ok {
		if userMd5 != calculatedMd5 {
			return "", datatype.BadDigest{
				ExpectedMD5:   userMd5,
				CalculatedMD5: calculatedMd5,
			}
		}
	}

	credential, err := data.(*minio.SignVerifyReader).Verify()
	if err != nil {
		return "", err
	}

	bucket, err := yig.MetaStorage.GetBucketInfo(bucketName)
	if err != nil {
		return "", err
	}

	if bucket.OwnerId != credential.UserId {
		return datatype.BucketAccessForbidden{Bucket: bucketName}
		// TODO validate bucket policy and ACL
	}

	object := meta.Object{
		Name:             objectName,
		BucketName:       bucketName,
		Location:         "", // TODO
		Pool:             poolName,
		OwnerId:          credential.UserId,
		Size:             bytesWritten,
		ObjectId:         mappedObjectName,
		LastModifiedTime: time.Now(),
		Etag:             calculatedMd5,
		ContentType:      metadata["Content-Type"],
		// TODO CustomAttributes
	}

	put, err := hrpc.NewPutStr(context.Background(), meta.OBJECT_TABLE,
		object.GetRowkey(), object.GetValues())
	if err != nil {
		return "", err
	}
	_, err = yig.MetaStorage.Hbase.Put(put)
	if err != nil {
		return "", err
	}
	return calculatedMd5, nil
}

func (yig *YigStorage) DeleteObject(bucket, object string) error {
	return nil
}
