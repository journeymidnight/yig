package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"

	"git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/iam"
	"git.letv.cn/yig/yig/meta"
	"git.letv.cn/yig/yig/redis"
	"github.com/cannium/gohbase/filter"
	"github.com/cannium/gohbase/hrpc"
	"github.com/xxtea/xxtea-go/xxtea"
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
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	put, err := hrpc.NewPutStr(ctx, meta.BUCKET_TABLE, bucketName, values)
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
		bucket, err := yig.MetaStorage.GetBucket(bucketName)
		if err != nil {
			yig.Logger.Println("Error get bucket: ", bucketName, ", with error", err)
			return ErrBucketAlreadyExists
		}
		if bucket.OwnerId == credential.UserId {
			return ErrBucketAlreadyOwnedByYou
		} else {
			return ErrBucketAlreadyExists
		}
	}
	err = yig.MetaStorage.AddBucketForUser(bucketName, credential.UserId)
	if err != nil { // roll back bucket table, i.e. remove inserted bucket
		yig.Logger.Println("Error AddBucketForUser: ", err)
		ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
		defer done()
		del, err := hrpc.NewDelStr(ctx, meta.BUCKET_TABLE, bucketName, values)
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
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.UserTable, credential.UserId)
	}
	return err
}

func (yig *YigStorage) SetBucketAcl(bucketName string, policy datatype.AccessControlPolicy, acl datatype.Acl,
	credential iam.Credential) error {

	if acl.CannedAcl == "" {
		newCannedAcl, err := datatype.GetCannedAclFromPolicy(policy)
		if err != nil {
			return err
		}
		acl = newCannedAcl
	}

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
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	put, err := hrpc.NewPutStr(ctx, meta.BUCKET_TABLE, bucketName, values)
	if err != nil {
		yig.Logger.Println("Error making hbase put: ", err)
		return err
	}
	_, err = yig.MetaStorage.Hbase.Put(put)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
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
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	put, err := hrpc.NewPutStr(ctx, meta.BUCKET_TABLE, bucketName, values)
	if err != nil {
		yig.Logger.Println("Error making hbase put: ", err)
		return err
	}
	_, err = yig.MetaStorage.Hbase.Put(put)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
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
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	put, err := hrpc.NewPutStr(ctx, meta.BUCKET_TABLE, bucketName, values)
	if err != nil {
		yig.Logger.Println("Error making hbase put: ", err)
		return err
	}
	_, err = yig.MetaStorage.Hbase.Put(put)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
	}
	return nil
}

func (yig *YigStorage) GetBucketCors(bucketName string,
	credential iam.Credential) (cors datatype.Cors, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName)
	if err != nil {
		return cors, err
	}
	if bucket.OwnerId != credential.UserId {
		err = ErrBucketAccessForbidden
		return
	}
	if len(bucket.CORS.CorsRules) == 0 {
		err = ErrNoSuchBucketCors
		return
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
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	put, err := hrpc.NewPutStr(ctx, meta.BUCKET_TABLE, bucketName, values)
	if err != nil {
		yig.Logger.Println("Error making hbase put: ", err)
		return err
	}
	_, err = yig.MetaStorage.Hbase.Put(put)
	if err != nil {
		return err
	}
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
	}
	return nil
}

func (yig *YigStorage) GetBucketVersioning(bucketName string, credential iam.Credential) (
	versioning datatype.Versioning, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName)
	if err != nil {
		return versioning, err
	}
	versioning.Status = helper.Ternary(bucket.Versioning == "Disabled",
		"", bucket.Versioning).(string)
	return
}

func (yig *YigStorage) GetBucketAcl(bucketName string, credential iam.Credential) (
        policy datatype.AccessControlPolicy, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName)
	if err != nil {
		return policy, err
	}
	if bucket.OwnerId != credential.UserId {
		err = ErrBucketAccessForbidden
		return
	}
	owner := datatype.Owner{ID: credential.UserId, DisplayName: credential.DisplayName}
	bucketOwner := datatype.Owner{}
	policy, err = datatype.CreatePolicyFromCanned(owner, bucketOwner, bucket.ACL)
	if err != nil {
		return policy, err
	}

	return
}

// For INTERNAL USE ONLY
func (yig *YigStorage) GetBucket(bucketName string) (meta.Bucket, error) {
	return yig.MetaStorage.GetBucket(bucketName)
}

func (yig *YigStorage) GetBucketInfo(bucketName string,
	credential iam.Credential) (bucket meta.Bucket, err error) {

	bucket, err = yig.MetaStorage.GetBucket(bucketName)
	if err != nil {
		return
	}
	if bucket.OwnerId != credential.UserId {
		switch bucket.ACL.CannedAcl {
		case "public-read", "public-read-write", "authenticated-read":
			break
		default:
			err = ErrBucketAccessForbidden
			return
		}
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

func (yig *YigStorage) DeleteBucket(bucketName string, credential iam.Credential) (err error) {
	bucket, err := yig.MetaStorage.GetBucket(bucketName)
	if err != nil {
		return err
	}
	if bucket.OwnerId != credential.UserId {
		return ErrBucketAccessForbidden
		// TODO validate bucket policy
	}

	// Check if bucket is empty
	prefixFilter := filter.NewPrefixFilter([]byte(bucketName))
	stopKey := []byte(bucketName)
	stopKey[len(stopKey)-1]++
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	scanRequest, err := hrpc.NewScanRangeStr(ctx, meta.OBJECT_TABLE,
		bucketName, string(stopKey),
		hrpc.Filters(prefixFilter), hrpc.NumberOfRows(1))
	if err != nil {
		return
	}
	scanResponse, err := yig.MetaStorage.Hbase.Scan(scanRequest)
	if err != nil {
		return
	}
	if len(scanResponse) != 0 {
		o, err := meta.ObjectFromResponse(scanResponse[0])
		if err != nil {
			return err
		}
		// to make sure the object is exactly from `bucketName` bucket,
		// not some `bucketNameAndSuffix` bucket
		if o.BucketName == bucketName {
			return ErrBucketNotEmpty
		}
	}

	values := map[string]map[string][]byte{
		meta.BUCKET_COLUMN_FAMILY: map[string][]byte{},
	}
	deleteRequest, err := hrpc.NewDelStr(ctx, meta.BUCKET_TABLE, bucketName, values)
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
		put, err := hrpc.NewPutStr(ctx, meta.BUCKET_TABLE, bucketName, values)
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
	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.UserTable, credential.UserId)
		yig.MetaStorage.Cache.Remove(redis.BucketTable, bucketName)
	}
	return nil
}

func (yig *YigStorage) ListObjects(credential iam.Credential, bucketName string,
	request datatype.ListObjectsRequest) (result meta.ListObjectsInfo, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName)
	helper.Debugln("GetBucket", bucket)
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
		startRowkey.WriteString(marker + meta.ObjectNameEnding)
	}
	stopKey := []byte(bucketName)
	stopKey[len(bucketName)-1]++

	comparator := filter.NewRegexStringComparator(
		"^"+bucketName+".."+request.Prefix+".*"+meta.ObjectNameEnding+".{8}"+"$",
		0x20, // Dot-all mode
		"ISO-8859-1",
		"JAVA", // regexp engine name, in `JAVA` or `JONI`
	)
	compareFilter := filter.NewCompareFilter(filter.Equal, comparator)
	rowFilter := filter.NewRowFilter(compareFilter)

	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	scanRequest, err := hrpc.NewScanRangeStr(ctx, meta.OBJECT_TABLE,
		startRowkey.String(), string(stopKey),
		// scan for max+2 rows to determine if results are truncated
		hrpc.Filters(rowFilter), hrpc.NumberOfRows(uint32(request.MaxKeys+2)))
	if err != nil {
		return
	}
	scanResponse, err := yig.MetaStorage.Hbase.Scan(scanRequest)
	if err != nil {
		return
	}
	if len(scanResponse) > 0 {
		var firstObject *meta.Object
		firstObject, err = meta.ObjectFromResponse(scanResponse[0])
		if err != nil {
			return
		}

		if marker == "" || (marker != "" && marker != firstObject.Name) {
			if len(scanResponse) > request.MaxKeys {
				result.IsTruncated = true
				var nextObject *meta.Object
				nextObject, err = meta.ObjectFromResponse(scanResponse[request.MaxKeys-1])
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
		} else if marker != "" && marker == firstObject.Name {
			if len(scanResponse) > (request.MaxKeys + 1) {
				result.IsTruncated = true
				var nextObject *meta.Object
				nextObject, err = meta.ObjectFromResponse(scanResponse[request.MaxKeys])
				if err != nil {
					return
				}
				if request.Version == 2 {
					result.NextMarker = meta.Encrypt(nextObject.Name)
				} else {
					result.NextMarker = nextObject.Name
				}
				scanResponse = scanResponse[1 : request.MaxKeys+1]
			} else {
				scanResponse = scanResponse[1:(len(scanResponse))]
			}
		}
	}

	var currentLevel int
	if request.Delimiter == "" {
		currentLevel = 0
	} else {
		currentLevel = strings.Count(request.Prefix, request.Delimiter)
	}

	objectMap := make(map[string]*meta.Object)
	prefixMap := make(map[string]int) // value is dummy, only need a set here
	for _, row := range scanResponse {
		var o *meta.Object
		o, err = meta.ObjectFromResponse(row)
		if err != nil {
			return
		}
		// FIXME: note in current implement YIG server would fetch objects of
		// various versions from HBase and filter them afterwards
		if request.Delimiter == "" {
			// save only the latest object version
			if savedVersion, ok := objectMap[o.Name]; !ok || savedVersion.LastModifiedTime.Before(o.LastModifiedTime) {
				objectMap[o.Name] = o
			}
		} else {
			level := strings.Count(o.Name, request.Delimiter)
			if level > currentLevel {
				split := strings.Split(o.Name, request.Delimiter)
				split = split[:currentLevel+1]
				prefix := strings.Join(split, request.Delimiter) + request.Delimiter
				prefixMap[prefix] = 1
			} else {
				// save only the latest object version
				// TODO: refactor, same as above
				if savedVersion, ok := objectMap[o.Name]; !ok || savedVersion.LastModifiedTime.Before(o.LastModifiedTime) {
					objectMap[o.Name] = o
				}
			}
		}
	}

	objectNames := helper.Keys(objectMap)
	sort.Strings(objectNames)
	objects := make([]datatype.Object, 0, len(objectNames))
	for _, objectName := range objectNames {
		o := objectMap[objectName]
		if o.DeleteMarker {
			// do not show deleted files in ListObjects
			continue
		}
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

	if request.EncodingType != "" { // only support "url" encoding for now
		result.Prefixes = helper.Map(result.Prefixes, func(s string) string {
			return url.QueryEscape(s)
		})
		result.NextMarker = url.QueryEscape(result.NextMarker)
	}

	return
}

// TODO: refactor, similar to ListObjects
// or not?
func (yig *YigStorage) ListVersionedObjects(credential iam.Credential, bucketName string,
	request datatype.ListObjectsRequest) (result meta.VersionedListObjectsInfo, err error) {

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

	var startRowkey bytes.Buffer
	startRowkey.WriteString(bucketName)
	if request.KeyMarker != "" {
		err = binary.Write(&startRowkey, binary.BigEndian,
			uint16(strings.Count(request.KeyMarker, "/")))
		if err != nil {
			return
		}
		startRowkey.WriteString(request.KeyMarker + meta.ObjectNameEnding)

		// TODO: refactor, same as in getObjectRowkeyPrefix
		if request.VersionIdMarker != "" {
			var versionBytes []byte
			versionBytes, err = hex.DecodeString(request.VersionIdMarker)
			if err != nil {
				return
			}
			decrypted := xxtea.Decrypt(versionBytes, meta.XXTEA_KEY)
			unixNanoTimestamp, errno := binary.Uvarint(decrypted)
			if errno <= 0 {
				err = ErrInvalidVersioning
				return
			}
			err = binary.Write(&startRowkey, binary.BigEndian,
				math.MaxUint64-unixNanoTimestamp)
			if err != nil {
				return
			}
		}
	}
	stopKey := []byte(bucketName)
	stopKey[len(stopKey)-1]++

	comparator := filter.NewRegexStringComparator(
		"^"+bucketName+".."+request.Prefix+".*"+meta.ObjectNameEnding+".{8}"+"$",
		0x20, // Dot-all mode
		"ISO-8859-1",
		"JAVA", // regexp engine name, in `JAVA` or `JONI`
	)
	compareFilter := filter.NewCompareFilter(filter.Equal, comparator)
	rowFilter := filter.NewRowFilter(compareFilter)

	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	scanRequest, err := hrpc.NewScanRangeStr(ctx, meta.OBJECT_TABLE,
		startRowkey.String(), string(stopKey), hrpc.Filters(rowFilter),
		// scan for max+1 rows to determine if results are truncated
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
		var nextObject *meta.Object
		nextObject, err = meta.ObjectFromResponse(scanResponse[request.MaxKeys])
		if err != nil {
			return
		}
		result.NextKeyMarker = nextObject.Name
		if !nextObject.NullVersion {
			result.NextVersionIdMarker = nextObject.GetVersionId()
		}
		scanResponse = scanResponse[:request.MaxKeys]
	}

	var currentLevel int
	if request.Delimiter == "" {
		currentLevel = 0
	} else {
		currentLevel = strings.Count(request.Prefix, request.Delimiter)
	}

	objects := make([]datatype.VersionedObject, 0, len(scanResponse))
	prefixMap := make(map[string]int) // value is dummy, only need a set here
	for _, row := range scanResponse {
		var o *meta.Object
		o, err = meta.ObjectFromResponse(row)
		if err != nil {
			return
		}
		// TODO: IsLatest
		object := datatype.VersionedObject{
			LastModified: o.LastModifiedTime.UTC().Format(meta.CREATE_TIME_LAYOUT),
			ETag:         "\"" + o.Etag + "\"",
			Size:         o.Size,
			StorageClass: "STANDARD",
		}
		if request.Delimiter == "" {
			object.Key = o.Name
		} else {
			level := strings.Count(o.Name, request.Delimiter)
			if level > currentLevel {
				split := strings.Split(o.Name, request.Delimiter)
				split = split[:currentLevel+1]
				prefix := strings.Join(split, request.Delimiter) + request.Delimiter
				prefixMap[prefix] = 1
				continue
			} else {
				object.Key = o.Name
			}
		}
		object.Key = strings.TrimPrefix(object.Key, request.Prefix)
		if request.EncodingType != "" { // only support "url" encoding for now
			object.Key = url.QueryEscape(object.Key)
		}
		if !o.NullVersion {
			object.VersionId = o.GetVersionId()
		} else {
			object.VersionId = "null"
		}
		if o.DeleteMarker {
			object.XMLName.Local = "DeleteMarker"
		} else {
			object.XMLName.Local = "Version"
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

	if request.EncodingType != "" { // only support "url" encoding for now
		result.Prefixes = helper.Map(result.Prefixes, func(s string) string {
			return url.QueryEscape(s)
		})
		result.NextKeyMarker = url.QueryEscape(result.NextKeyMarker)
	}

	return
}
