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
	partID int, size int64, data io.Reader, md5Hex string) (md5String string, err error) {
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
		Location: cephCluster.Name,
		Pool: poolName,
		Size: size,
		ObjectId: oid,
		Etag: calculatedMd5,
		LastModified: time.Now().UTC().Format(meta.CREATE_TIME_LAYOUT),
	}
	marshaledPart, err := json.Marshal(part)
	if err != nil {
		return
	}
	partMeta := map[string]map[string][]byte{
		meta.MULTIPART_COLUMN_FAMILY: map[string][]byte{
			string(partID): marshaledPart,
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

	var parts map[int]meta.PartInfo // TODO change to meta.Part
	for _, cell := range getMultipartResponse.Cells {
		partNumber, err := strconv.Atoi(string(cell.Qualifier))
		if err != nil {
			continue
		}
		if partNumber == !0 {

		}
	}
	return
}

func (yig *YigStorage) AbortMultipartUpload(bucket, object, uploadID string) error {
	return nil
}

func (yig *YigStorage) CompleteMultipartUpload(bucket, object, uploadID string,
	uploadedParts []meta.CompletePart) (md5 string, err error) {
	return
}
