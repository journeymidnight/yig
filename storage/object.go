package storage

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"git.letv.cn/yig/yig/meta"
	"git.letv.cn/yig/yig/minio/datatype"
	"git.letv.cn/yig/yig/signature"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
	"io"
	"strings"
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

func (yig *YigStorage) GetObject(object datatype.ObjectInfo, startOffset int64,
	length int64, writer io.Writer) (err error) {
	cephCluster, ok := yig.DataStorage[object.Location]
	if !ok {
		return errors.New("Cannot find specified ceph cluster: " + object.Location)
	}
	err = cephCluster.get(object.PoolName, object.ObjectId, startOffset, length, writer)
	return
}

func (yig *YigStorage) GetObjectInfo(bucket, object string) (objInfo datatype.ObjectInfo, err error) {
	objectRowkeyPrefix, err := meta.GetObjectRowkeyPrefix(bucket, object)
	if err != nil {
		return
	}
	filter := filter.NewPrefixFilter(objectRowkeyPrefix)
	scanRequest, err := hrpc.NewScanRangeStr(context.Background(), meta.OBJECT_TABLE,
		string(objectRowkeyPrefix), "", hrpc.Filters(filter), hrpc.NumberOfRows(1))
	if err != nil {
		return
	}
	scanResponse, err := yig.MetaStorage.Hbase.Scan(scanRequest)
	if err != nil {
		return
	}
	if len(scanResponse) == 0 {
		err = datatype.ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
		return
	}
	for _, cell := range scanResponse[0].Cells {
		if !bytes.HasPrefix(cell.Row, objectRowkeyPrefix) {
			err = datatype.ObjectNotFound{
				Bucket: bucket,
				Object: object,
			}
			return
		}
		switch string(cell.Qualifier) {
		case "lastModified":
			objInfo.ModTime, err = time.Parse(meta.CREATE_TIME_LAYOUT, string(cell.Value))
			if err != nil {
				return
			}
		case "size":
			err = binary.Read(bytes.NewReader(cell.Value), binary.BigEndian, &objInfo.Size)
			if err != nil {
				return
			}
		case "content-type":
			objInfo.ContentType = string(cell.Value)
		case "etag":
			objInfo.MD5Sum = string(cell.Value)
		case "oid":
			objInfo.ObjectId = string(cell.Value)
		case "location":
			objInfo.Location = string(cell.Value)
		case "pool":
			objInfo.PoolName = string(cell.Value)
		}
	}
	objInfo.Bucket = bucket
	objInfo.Name = object
	if strings.HasSuffix(object, "/") {
		objInfo.IsDir = true
	} else {
		objInfo.IsDir = false
	}

	return
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

	credential, err := data.(*signature.SignVerifyReader).Verify()
	if err != nil {
		return "", err
	}

	bucket, err := yig.MetaStorage.GetBucketInfo(bucketName)
	if err != nil {
		return "", err
	}

	if bucket.OwnerId != credential.UserId {
		return "", datatype.BucketAccessForbidden{Bucket: bucketName}
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

func (yig *YigStorage) DeleteObject(bucket, object string) error {
	// TODO validate policy and ACL
	objectRowkeyPrefix, err := meta.GetObjectRowkeyPrefix(bucket, object)
	if err != nil {
		return err
	}
	filter := filter.NewPrefixFilter(objectRowkeyPrefix)
	scanRequest, err := hrpc.NewScanRangeStr(context.Background(), meta.OBJECT_TABLE,
		string(objectRowkeyPrefix), "", hrpc.Filters(filter), hrpc.NumberOfRows(1))
	if err != nil {
		return err
	}
	// TODO abstract this part
	// TODO versioning
	scanResponse, err := yig.MetaStorage.Hbase.Scan(scanRequest)
	if err != nil {
		return err
	}
	if len(scanResponse) == 0 {
		return datatype.ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	}
	rowkeyToDelete := string(scanResponse[0].Cells[0].Row)
	var oidToDelete, location, poolName []byte
	for _, cell := range scanResponse[0].Cells {
		switch string(cell.Qualifier) {
		case "oid":
			oidToDelete = cell.Value
		case "location":
			location = cell.Value
		case "pool":
			poolName = cell.Value
		}
	}
	valuesToDelete := map[string]map[string][]byte{
		meta.OBJECT_COLUMN_FAMILY: map[string][]byte{},
	}
	deleteRequest, err := hrpc.NewDelStr(context.Background(), meta.OBJECT_TABLE,
		rowkeyToDelete, valuesToDelete)
	if err != nil {
		return err
	}
	_, err = yig.MetaStorage.Hbase.Delete(deleteRequest)
	if err != nil {
		return err
	}

	garbageCollectionValues := map[string]map[string][]byte{
		meta.GARBAGE_COLLECTION_COLUMN_FAMILY: map[string][]byte{
			"location": location,
			"pool":     poolName,
			"oid":      oidToDelete,
		},
	}
	garbageCollectionRowkey, err := meta.GetGarbageCollectionRowkey(bucket, object)
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
		yig.Logger.Println("Inconsistent data: object with oid ", oidToDelete,
			"should be removed in ", location, poolName)
		return err
	}
	return nil
}
