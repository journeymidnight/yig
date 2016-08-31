package storage

import (
	"bytes"
	"encoding/binary"
	"git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/iam"
	"git.letv.cn/yig/yig/meta"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
	"net/url"
	"sort"
	"strings"
	"time"
)

func (yig *YigStorage) MakeBucket(bucketName string, acl datatype.Acl,
	credential iam.Credential) error {

	now := time.Now().UTC()
	bucket := meta.Bucket{
		Name:       bucketName,
		CreateTime: now,
		OwnerId:    credential.UserId,
		ACL:        acl,
		Versioning: "Disabled", // it's the default
	}
	values, err := bucket.GetValues()
	if err != nil {
		return err
	}
	put, err := hrpc.NewPutStr(context.Background(), meta.BUCKET_TABLE, bucketName, values)
	if err != nil {
		yig.Logger.Println("Error making hbase put: ", err)
		return err
	}
	processed, err := yig.MetaStorage.Hbase.CheckAndPut(put, meta.BUCKET_COLUMN_FAMILY,
		"UID", []byte{})
	if err != nil {
		yig.Logger.Println("Error making hbase checkandput: ", err)
		return err
	}
	if !processed { // bucket already exists, return accurate message
		family := map[string][]string{meta.BUCKET_COLUMN_FAMILY: []string{"UID"}}
		get, err := hrpc.NewGetStr(context.Background(), meta.BUCKET_TABLE, bucketName,
			hrpc.Families(family))
		if err != nil {
			yig.Logger.Println("Error making hbase get: ", err)
			return err
		}
		b, err := yig.MetaStorage.Hbase.Get(get)
		if err != nil {
			yig.Logger.Println("Error get bucket: ", bucketName, "with error: ", err)
			return ErrBucketAlreadyExists
		}
		if string(b.Cells[0].Value) == credential.UserId {
			return ErrBucketAlreadyOwnedByYou
		} else {
			return ErrBucketAlreadyExists
		}
	}
	err = yig.MetaStorage.AddBucketForUser(bucketName, credential.UserId)
	if err != nil { // roll back bucket table, i.e. remove inserted bucket
		yig.Logger.Println("Error AddBucketForUser: ", err)
		del, err := hrpc.NewDelStr(context.Background(), meta.BUCKET_TABLE, bucketName, values)
		if err != nil {
			yig.Logger.Println("Error making hbase del: ", err)
			yig.Logger.Println("Leaving junk bucket unremoved: ", bucketName)
			return err
		}
		_, err = yig.MetaStorage.Hbase.Delete(del)
		if err != nil {
			yig.Logger.Println("Error deleting: ", err)
			yig.Logger.Println("Leaving junk bucket unremoved: ", bucketName)
			return err
		}
	}
	return err
}

func (yig *YigStorage) SetBucketAcl(bucketName string, acl datatype.Acl,
	credential iam.Credential) error {

	bucket, err := yig.MetaStorage.GetBucket(bucketName)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.ACL = acl
	values, err := bucket.GetValues()
	if err != nil {
		return err
	}
	put, err := hrpc.NewPutStr(context.Background(), meta.BUCKET_TABLE, bucketName, values)
	if err != nil {
		yig.Logger.Println("Error making hbase put: ", err)
		return err
	}
	_, err = yig.MetaStorage.Hbase.Put(put)
	if err != nil {
		return err
	}
	return nil
}

func (yig *YigStorage) SetBucketCors(bucketName string, cors datatype.Cors,
	credential iam.Credential) error {

	bucket, err := yig.MetaStorage.GetBucket(bucketName)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.CORS = cors
	values, err := bucket.GetValues()
	if err != nil {
		return err
	}
	put, err := hrpc.NewPutStr(context.Background(), meta.BUCKET_TABLE, bucketName, values)
	if err != nil {
		yig.Logger.Println("Error making hbase put: ", err)
		return err
	}
	_, err = yig.MetaStorage.Hbase.Put(put)
	if err != nil {
		return err
	}
	return nil
}

func (yig *YigStorage) DeleteBucketCors(bucketName string, credential iam.Credential) error {
	bucket, err := yig.MetaStorage.GetBucket(bucketName)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.CORS = datatype.Cors{}
	values, err := bucket.GetValues()
	if err != nil {
		return err
	}
	put, err := hrpc.NewPutStr(context.Background(), meta.BUCKET_TABLE, bucketName, values)
	if err != nil {
		yig.Logger.Println("Error making hbase put: ", err)
		return err
	}
	_, err = yig.MetaStorage.Hbase.Put(put)
	if err != nil {
		return err
	}
	return nil
}

func (yig *YigStorage) GetBucketCors(bucketName string, credential iam.Credential) (datatype.Cors, error) {
	var cors datatype.Cors
	bucket, err := yig.MetaStorage.GetBucket(bucketName)
	if err != nil {
		return cors, err
	}
	return bucket.CORS, nil
}

func (yig *YigStorage) SetBucketVersioning(bucketName string, versioning datatype.Versioning,
	credential iam.Credential) error {

	bucket, err := yig.MetaStorage.GetBucket(bucketName)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
	}
	bucket.Versioning = versioning.Status
	values, err := bucket.GetValues()
	if err != nil {
		return err
	}
	put, err := hrpc.NewPutStr(context.Background(), meta.BUCKET_TABLE, bucketName, values)
	if err != nil {
		yig.Logger.Println("Error making hbase put: ", err)
		return err
	}
	_, err = yig.MetaStorage.Hbase.Put(put)
	if err != nil {
		return err
	}
	return nil
}

func (yig *YigStorage) GetBucketVersioning(bucketName string, credential iam.Credential) (
	versioning datatype.Versioning, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName)
	if err != nil {
		return versioning, err
	}
	return datatype.Versioning{
		Status: bucket.Versioning,
	}, nil
}

// For INTERNAL USE ONLY
func (yig *YigStorage) GetBucket(bucketName string) (meta.Bucket, error) {
	return yig.MetaStorage.GetBucket(bucketName)
}

func (yig *YigStorage) GetBucketInfo(bucketName string,
	credential iam.Credential) (bucketInfo meta.Bucket, err error) {
	bucketInfo, err = yig.MetaStorage.GetBucket(bucketName)
	if err != nil {
		return
	}
	if bucketInfo.OwnerId != credential.UserId {
		err = ErrBucketAccessForbidden
		return
		// TODO validate bucket policy
	}
	return
}

func (yig *YigStorage) ListBuckets(credential iam.Credential) (buckets []meta.Bucket, err error) {
	bucketNames, err := yig.MetaStorage.GetUserBuckets(credential.UserId)
	if err != nil {
		return
	}
	for _, bucketName := range bucketNames {
		bucket, err := yig.MetaStorage.GetBucket(bucketName)
		if err != nil {
			return buckets, err
		}
		buckets = append(buckets, bucket)
	}
	return
}

func (yig *YigStorage) DeleteBucket(bucketName string, credential iam.Credential) error {
	bucket, err := yig.MetaStorage.GetBucket(bucketName)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
		// TODO validate bucket policy
	}
	// TODO validate bucket is empty

	values := map[string]map[string][]byte{
		meta.BUCKET_COLUMN_FAMILY: map[string][]byte{},
	}
	deleteRequest, err := hrpc.NewDelStr(context.Background(), meta.BUCKET_TABLE, bucketName, values)
	if err != nil {
		return err
	}
	_, err = yig.MetaStorage.Hbase.Delete(deleteRequest)
	if err != nil {
		return err
	}

	err = yig.MetaStorage.RemoveBucketForUser(bucketName, credential.UserId)
	if err != nil { // roll back bucket table, i.e. re-add removed bucket entry
		values, err = bucket.GetValues()
		if err != nil {
			return err
		}
		put, err := hrpc.NewPutStr(context.Background(), meta.BUCKET_TABLE, bucketName, values)
		if err != nil {
			yig.Logger.Println("Error making hbase put: ", err)
			yig.Logger.Println("Inconsistent data: bucket ", bucketName,
				"should be removed for user ", credential.UserId)
			return err
		}
		_, err = yig.MetaStorage.Hbase.Put(put)
		if err != nil {
			yig.Logger.Println("Error making hbase put: ", err)
			yig.Logger.Println("Inconsistent data: bucket ", bucketName,
				"should be removed for user ", credential.UserId)
			return err
		}
	}
	return nil
}

func (yig *YigStorage) ListObjects(credential iam.Credential, bucketName string,
	request datatype.ListObjectsRequest) (result meta.ListObjectsInfo, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName)
	if err != nil {
		return
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
	// TODO validate user policy and ACL

	var marker string
	if request.Version == 2 {
		if request.ContinuationToken != "" {
			marker, err = meta.Decrypt(request.ContinuationToken)
			if err != nil {
				err = ErrInvalidContinuationToken
				return
			}
		} else {
			marker = request.StartAfter
		}
	} else { // version 1
		marker = request.Marker
	}

	var startRowkey bytes.Buffer
	startRowkey.WriteString(bucketName)
	if marker != "" {
		err = binary.Write(&startRowkey, binary.BigEndian, uint16(strings.Count(marker, "/")))
		if err != nil {
			return
		}
		err = binary.Write(&startRowkey, binary.BigEndian, uint16(len([]byte(marker))))
		if err != nil {
			return
		}
		startRowkey.WriteString(marker)
	}

	comparator := filter.NewRegexStringComparator(
		"^"+bucketName+"...."+request.Prefix+".*"+".{8}"+"$",
		0x20, // Dot-all mode
		"ISO-8859-1",
		"JAVA", // regexp engine name, in `JAVA` or `JONI`
	)
	compareFilter := filter.NewCompareFilter(filter.Equal, comparator)
	rowFilter := filter.NewRowFilter(compareFilter)

	scanRequest, err := hrpc.NewScanRangeStr(context.Background(), meta.OBJECT_TABLE,
		// scan for max+1 rows to determine if results are truncated
		startRowkey.String(), "", hrpc.Filters(rowFilter),
		hrpc.NumberOfRows(uint32(request.MaxKeys+1)))
	if err != nil {
		return
	}
	scanResponse, err := yig.MetaStorage.Hbase.Scan(scanRequest)
	if err != nil {
		return
	}
	if len(scanResponse) > request.MaxKeys {
		result.IsTruncated = true
		var nextObject meta.Object
		nextObject, err = meta.ObjectFromResponse(scanResponse[request.MaxKeys], bucketName)
		if err != nil {
			return
		}
		if request.Version == 2 {
			result.NextMarker = meta.Encrypt(nextObject.Name)
		} else {
			result.NextMarker = nextObject.Name
		}
		scanResponse = scanResponse[:request.MaxKeys]
	}

	var currentLevel int
	if request.Delimiter == "" {
		currentLevel = 0
	} else {
		currentLevel = strings.Count(request.Prefix, request.Delimiter)
	}

	objectMap := make(map[string]meta.Object)
	prefixMap := make(map[string]int) // value is dummy, only need a set here
	for _, row := range scanResponse {
		var o meta.Object
		o, err = meta.ObjectFromResponse(row, bucketName)
		if err != nil {
			return
		}
		// FIXME: note in current implement YIG server would fetch objects of
		// various versions
		if request.Delimiter == "" {
			objectMap[o.Name] = o
		} else {
			level := strings.Count(o.Name, request.Delimiter)
			if level > currentLevel {
				split := strings.Split(o.Name, request.Delimiter)
				split = split[:currentLevel+1]
				prefix := strings.Join(split, request.Delimiter) + request.Delimiter
				prefixMap[prefix] = 1
			} else {
				objectMap[o.Name] = o
			}
		}
	}

	objectNames := helper.Keys(objectMap)
	sort.Strings(objectNames)
	objects := make([]datatype.Object, 0, len(objectNames))
	for _, objectName := range objectNames {
		o := objectMap[objectName]
		object := datatype.Object{
			LastModified: o.LastModifiedTime.UTC().Format(meta.CREATE_TIME_LAYOUT),
			ETag:         "\"" + o.Etag + "\"",
			Size:         o.Size,
			StorageClass: "STANDARD",
		}
		if request.EncodingType == "" {
			object.Key = strings.TrimPrefix(o.Name, request.Prefix)
		} else { // only support "url" encoding for now
			object.Key = url.QueryEscape(strings.TrimPrefix(o.Name, request.Prefix))
		}

		if request.FetchOwner {
			var owner iam.Credential
			owner, err = iam.GetCredentialByUserId(o.OwnerId)
			if err != nil {
				return
			}
			object.Owner = datatype.Owner{
				ID:          owner.UserId,
				DisplayName: owner.DisplayName,
			}
		}
		objects = append(objects, object)
	}
	result.Objects = objects

	prefixes := helper.Keys(prefixMap)
	sort.Strings(prefixes)
	result.Prefixes = prefixes

	return
}
