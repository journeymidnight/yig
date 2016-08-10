package storage

import (
	"git.letv.cn/yig/yig/meta"
	"io"
	"github.com/satori/go.uuid"
	"encoding/json"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
	"git.letv.cn/yig/yig/iam"
	. "git.letv.cn/yig/yig/error"
	"crypto/md5"
	"encoding/hex"
	"git.letv.cn/yig/yig/signature"
	"time"
	"strconv"
)

const (
	MAX_PART_SIZE = 5 << 30 // 5GB
	MIN_PART_SIZE = 128 << 10 // 128KB
	MAX_PART_NUMBER = 10000
)

func (yig *YigStorage) ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker,
	delimiter string, maxUploads int) (result meta.ListMultipartsInfo, err error) {
	return
}

func (yig *YigStorage) NewMultipartUpload(credential iam.Credential, bucketName, objectName string,
	metadata map[string]string) (uploadId string, err error) {
	bucket, err := yig.MetaStorage.GetBucketInfo(bucketName)
	if err != nil {
		return
	}
	if bucket.OwnerId != credential.UserId {
		return "", ErrBucketAccessForbidden
		// TODO validate policy and ACL
	}

	uploadId = string(uuid.NewV4())
	metadata["InitiatorId"] = credential.UserId
	metadata["OwnerId"] = bucket.OwnerId
	marshaledMeta, err := json.Marshal(metadata)
	if err != nil {
		return
	}
	newMultipart := map[string]map[string][]byte {
		meta.MULTIPART_COLUMN_FAMILY: map[string][]byte {
			"0": marshaledMeta,
		},
	}
	newMultipartPut, err := hrpc.NewPutStr(context.Background(), meta.MULTIPART_TABLE,
		bucketName + objectName +uploadId, newMultipart)
	if err != nil {
		return
	}
	_, err = yig.MetaStorage.Hbase.Put(newMultipartPut)
	if err != nil {
		return
	}
	return
}

func (yig *YigStorage) PutObjectPart(bucketName, objectName, uploadId string,
	partId int, size int64, data io.Reader, md5Hex string) (md5String string, err error) {

	multipartMeta := map[string][]string{meta.MULTIPART_COLUMN_FAMILY: []string{"0"}}
	getMultipartRequest, err := hrpc.NewGetStr(context.Background(), meta.MULTIPART_TABLE,
		bucketName + objectName + uploadId, multipartMeta)
	if err != nil {
		return
	}
	getMultipartResponse, err := yig.MetaStorage.Hbase.Get(getMultipartRequest)
	if err != nil {
		return
	}
	if len(getMultipartResponse.Cells) == 0 {
		err = ErrNoSuchUpload
		return
	}
	if size > MAX_PART_SIZE {
		err = ErrEntityTooLarge
		return
	}

	md5Writer := md5.New()
	limitedDataReader := io.LimitReader(data, size)
	cephCluster, poolName := yig.PickOneClusterAndPool(bucketName, objectName, size)
	oid := cephCluster.GetUniqUploadName()
	storageReader := io.TeeReader(limitedDataReader, md5Writer)
	bytesWritten, err := cephCluster.put(poolName, oid, storageReader)
	if err != nil {
		return
	}
	if bytesWritten < size {
		err = ErrIncompleteBody
		return
	}

	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	if md5Hex != calculatedMd5 {
		err = ErrBadDigest
		return
	}

	_, err = data.(*signature.SignVerifyReader).Verify()
	if err != nil {
		return
	} // TODO validate policy and ACL

	part := meta.Part{
		PartNumber: partId,
		Location: cephCluster.Name,
		Pool: poolName,
		Size: size,
		ObjectId: oid,
		Etag: calculatedMd5,
		LastModified: time.Now().UTC(),
	}
	marshaledPart, err := json.Marshal(part)
	if err != nil {
		return
	}
	partMeta := map[string]map[string][]byte{
		meta.MULTIPART_COLUMN_FAMILY: map[string][]byte{
			strconv.Itoa(partId): marshaledPart,
		},
	}
	partMetaPut, err := hrpc.NewPutStr(context.Background(), meta.MULTIPART_TABLE,
		bucketName + objectName + uploadId, partMeta)
	if err != nil {
		return
	}
	_, err = yig.MetaStorage.Hbase.Put(partMetaPut)
	if err != nil {
		// TODO remove object in Ceph
		return
	}
	return
}

func (yig *YigStorage) ListObjectParts(credential iam.Credential, bucketName, objectName, uploadId string,
	partNumberMarker int, maxParts int) (result meta.ListPartsInfo, err error) {
	getMultipartRequest, err := hrpc.NewGetStr(context.Background(), meta.MULTIPART_TABLE,
		bucketName + objectName + uploadId)
	if err != nil {
		return
	}
	getMultipartResponse, err := yig.MetaStorage.Hbase.Get(getMultipartRequest)
	if err != nil {
		return
	}
	if len(getMultipartResponse.Cells) == 0 {
		err = ErrNoSuchUpload
		return
	}

	var parts map[int]meta.Part
	for _, cell := range getMultipartResponse.Cells {
		partNumber, err := strconv.Atoi(string(cell.Qualifier))
		if err != nil {
			return
		}
		if partNumber == !0 {
			var p meta.Part
			err = json.Unmarshal(cell.Value, p)
			if err != nil {
				return
			}
			parts[partNumber] = p
		} else {
			var metadata map[string]string
			err = json.Unmarshal(cell.Value, metadata)
			if err != nil {
				return
			}
			result.InitiatorId = metadata["InitiatorId"]
			result.OwnerId = metadata["OwnerId"]
		}
	}
	partCount := 0
	for i := partNumberMarker; i <= MAX_PART_NUMBER; i++ {
		if p, ok := parts[i]; ok {
			result.Parts = append(result.Parts, p)

			partCount++
			if partCount > maxParts + 1 {
				break
			}
		}
	}
	if partCount == maxParts + 1 {
		result.IsTruncated = true
		result.Parts = result.Parts[:maxParts]
	}

	result.Bucket = bucketName
	result.Object = objectName
	result.UploadID = uploadId
	result.StorageClass = "STANDARD"
	result.PartNumberMarker = partNumberMarker
	result.MaxParts = maxParts

	return
}

func (yig *YigStorage) AbortMultipartUpload(credential iam.Credential,
bucketName, objectName, uploadId string) error {

	bucket, err := yig.MetaStorage.GetBucketInfo(bucketName)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
		// TODO validate bucket policy
	}

	values := map[string]map[string][]byte {
		meta.MULTIPART_COLUMN_FAMILY: map[string][]byte{},
	}
	deleteRequest, err := hrpc.NewDelStr(context.Background(), meta.MULTIPART_TABLE,
		bucketName + objectName + uploadId, values)
	if err != nil {
		return err
	}
	_, err = yig.MetaStorage.Hbase.Delete(deleteRequest)
	if err != nil {
		return err
	}
	return nil
}

func (yig *YigStorage) CompleteMultipartUpload(credential iam.Credential,
bucketName, objectName, uploadId string, uploadedParts []meta.CompletePart) (etagString string, err error) {

	getMultipartRequest, err := hrpc.NewGetStr(context.Background(), meta.MULTIPART_TABLE,
		bucketName + objectName + uploadId)
	if err != nil {
		return
	}
	getMultipartResponse, err := yig.MetaStorage.Hbase.Get(getMultipartRequest)
	if err != nil {
		return
	}
	if len(getMultipartResponse.Cells) == 0 {
		return "", ErrNoSuchUpload
	}

	var parts map[int]meta.Part
	var metadata map[string]string
	for _, cell := range getMultipartResponse.Cells {
		partNumber, err := strconv.Atoi(string(cell.Qualifier))
		if err != nil {
			return
		}
		if partNumber == !0 {
			var p meta.Part
			err = json.Unmarshal(cell.Value, p)
			if err != nil {
				return
			}
			parts[partNumber] = p
		} else {
			err = json.Unmarshal(cell.Value, metadata)
			if err != nil {
				return
			}
		}
	}
	md5Writer := md5.New()
	totalSize := 0
	for i := 0; i < len(uploadedParts); i++ {
		if uploadedParts[i].PartNumber != i + 1 {
			return "", ErrInvalidPart
		}
		part, ok := parts[i+1]
		if !ok {
			return "", ErrInvalidPart
		}
		if part.Size < MIN_PART_SIZE && part.PartNumber != len(uploadedParts) {
			return "", meta.PartTooSmall{
				PartSize: part.Size,
				PartNumber: part.PartNumber,
				PartETag: part.Etag,
			}
		}
		if part.Etag != uploadedParts[i].ETag {
			return "", ErrInvalidPart
		}
		etagBytes, err := hex.DecodeString(part.Etag)
		if err != nil {
			return "", ErrInvalidPart
		}
		totalSize += part.Size
		md5Writer.Write(etagBytes)
	}
	etagString = hex.EncodeToString(md5Writer.Sum(nil))
	etagString += "-" + strconv.Itoa(len(uploadedParts))
	// See http://stackoverflow.com/questions/12186993/what-is-the-algorithm-to-compute-the-amazon-s3-etag-for-a-file-larger-than-5gb
	// for how to calculate multipart Etag

	// Add to objects table
	contentType, ok := metadata["Content-Type"]
	if !ok {
		contentType = "application/octet-stream"
	}
	object := meta.Object{
		Name: objectName,
		BucketName: bucketName,
		OwnerId: metadata["OwnerId"],
		Size: totalSize,
		LastModifiedTime: time.Now().UTC(),
		Etag: etagString,
		ContentType: contentType,
		Parts: parts,
	}
	rowkey, err := object.GetRowkey()
	if err != nil {
		return
	}
	values, err := object.GetValues()
	if err != nil {
		return
	}
	put, err := hrpc.NewPutStr(context.Background(), meta.OBJECT_TABLE, rowkey, values)
	if err != nil {
		return
	}
	_, err = yig.MetaStorage.Hbase.Put(put)
	if err != nil {
		return
	}

	// Remove from multiparts table
	deleteValues := map[string]map[string][]byte {
		meta.MULTIPART_COLUMN_FAMILY: map[string][]byte{},
	}
	deleteRequest, err := hrpc.NewDelStr(context.Background(), meta.MULTIPART_TABLE,
		bucketName + objectName + uploadId, deleteValues)
	if err != nil {
		return
	}
	_, err = yig.MetaStorage.Hbase.Delete(deleteRequest)
	if err != nil { // rollback objects table
		objectDeleteValues := object.GetValuesForDelete()
		objectDeleteRequest, err := hrpc.NewDelStr(context.Background(), meta.OBJECT_TABLE,
			rowkey, objectDeleteValues)
		if err != nil {
			return
		}
		_, err = yig.MetaStorage.Hbase.Delete(objectDeleteRequest)
		if err != nil {
			yig.Logger.Println("Error deleting object: ", err)
			yig.Logger.Println("Inconsistent data: object with rowkey ", rowkey,
				"should be removed in HBase")
		}
		return
	}
	return
}
