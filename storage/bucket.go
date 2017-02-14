package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"math"
	"net/url"
	"strings"
	"time"

	"legitlab.letv.cn/yig/yig/api/datatype"
	. "legitlab.letv.cn/yig/yig/error"
	"legitlab.letv.cn/yig/yig/helper"
	"legitlab.letv.cn/yig/yig/iam"
	"legitlab.letv.cn/yig/yig/meta"
	"legitlab.letv.cn/yig/yig/redis"
	"github.com/cannium/gohbase/filter"
	"github.com/cannium/gohbase/hrpc"
	"github.com/xxtea/xxtea-go/xxtea"
	"unicode/utf8"
	"strconv"
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
		bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
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

	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
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

	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
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
	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
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

	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
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

	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
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

	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
	if err != nil {
		return versioning, err
	}
	versioning.Status = helper.Ternary(bucket.Versioning == "Disabled",
		"", bucket.Versioning).(string)
	return
}

func (yig *YigStorage) GetBucketAcl(bucketName string, credential iam.Credential) (
        policy datatype.AccessControlPolicy, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
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
	return yig.MetaStorage.GetBucket(bucketName, true)
}

func (yig *YigStorage) GetBucketInfo(bucketName string,
	credential iam.Credential) (bucket meta.Bucket, err error) {

	bucket, err = yig.MetaStorage.GetBucket(bucketName, true)
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
	bucketNames, err := yig.MetaStorage.GetUserBuckets(credential.UserId, true)
	if err != nil {
		return
	}
	for _, bucketName := range bucketNames {
		bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
		if err != nil {
			return buckets, err
		}
		buckets = append(buckets, bucket)
	}
	return
}

func (yig *YigStorage) DeleteBucket(bucketName string, credential iam.Credential) (err error) {
	bucket, err := yig.MetaStorage.GetBucket(bucketName, false)
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

func (yig *YigStorage) listObjects(credential iam.Credential, bucketName string,
        request datatype.ListObjectsRequest) (retObjects []*meta.Object, prefixes []string, truncated bool,
        nextMarker, nextVerIdMarker string, err error) {

	var marker string
	var verIdMarker string
	if request.Versioned {
		marker = request.KeyMarker
		verIdMarker = request.VersionIdMarker
	} else if request.Version == 2 {
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
	helper.Debugln("Prefix:", request.Prefix, "Marker:", request.Marker, "MaxKeys:",
		request.MaxKeys, "Delimiter:", request.Delimiter, "Version:", request.Version,
	        "keyMarker:", request.KeyMarker, "versionIdMarker:", request.VersionIdMarker)

	var exit bool
	var count int
	truncated = true
	currMarker := marker
	var currVerMarkerNum uint64
	if verIdMarker == "null" {
		objMap, e := yig.MetaStorage.GetObjectMap(bucketName, marker)
		if e != nil {
			err = e
			return
		}
		verIdMarker = objMap.NullVerId
	}
	if verIdMarker != "" {
		var versionBytes []byte
		versionBytes, err = hex.DecodeString(verIdMarker)
		if err == nil {
			decrypted := xxtea.Decrypt(versionBytes, meta.XXTEA_KEY)
			unixNanoTimestamp, e := strconv.ParseUint(string(decrypted), 10, 64)
			if e != nil {
				helper.Debugln("Error convert version id to int")
				err = ErrInvalidVersioning
				return
			}
			currVerMarkerNum = unixNanoTimestamp
		} else {
			err = nil
			helper.Debugln("Error decoding version id, skip to next object")
			currVerMarkerNum = 0
		}
	}
	var biggerThanDelim string
	var skipAfterDelim string
	var skipOldVerObj string
	objectMap := make(map[string]*meta.Object)
	commonPrefixes := make(map[string]bool)
	if len(request.Delimiter) != 0 {
		r, _ := utf8.DecodeRune([]byte(request.Delimiter))
		r = r + 1
		buf := make([]byte, 3)
		utf8.EncodeRune(buf, r)
		biggerThanDelim = string(buf)
		helper.Debugln("list objects, biggerThanDelim:", biggerThanDelim)
	}

	var newMarker bool
	if len(request.Delimiter) != 0 && len(request.Prefix) < len(currMarker){
		len := len(request.Prefix)
		subStr := currMarker[len:]
		idx := strings.Index(subStr, request.Delimiter)
		if idx != -1 {
			newMarker = true
			currMarker = currMarker[0:(len + idx)]
			currMarker += biggerThanDelim
			currVerMarkerNum = 0
			helper.Debugln("sub:", subStr, "len", len, "idx", idx, "currMarker", currMarker)
		}
	}
	if currMarker!= "" && !newMarker {
		if !request.Versioned || currVerMarkerNum == 0 {
			currMarker += meta.ObjectNameSmallestStr
		} else {
			currVerMarkerNum -= 1
		}
	}

	for ;truncated && count <= request.MaxKeys; {
		// Because start rowkey is included in scan result, update currMarker
		if strings.Compare(skipAfterDelim, currMarker) > 0 {
			currMarker = skipAfterDelim
			currVerMarkerNum = 0
			helper.Debugln("set new currMarker:", currMarker)
		}
		if strings.Compare(skipOldVerObj, currMarker) > 0 {
			currMarker = skipOldVerObj
			currVerMarkerNum = 0
			helper.Debugln("set new currMarker:", currMarker)
		}

		var startRowkey bytes.Buffer
		startRowkey.WriteString(bucketName + meta.ObjectNameSeparator)
		if currMarker != "" {
			startRowkey.WriteString(currMarker)
		}
		if currVerMarkerNum != 0 {
			startRowkey.WriteString(meta.ObjectNameSeparator)
			err = binary.Write(&startRowkey, binary.BigEndian,
				math.MaxUint64-currVerMarkerNum)
			if err != nil {
				return
			}
		}
		stopKey := []byte(bucketName)
		stopKey[len(bucketName)-1]++
		comparator := filter.NewRegexStringComparator(
			"^"+bucketName+ meta.ObjectNameSeparator +request.Prefix+".*",
			0x20, // Dot-all mode
			"UTF-8",
			"JAVA", // regexp engine name, in `JAVA` or `JONI`
		)
		compareFilter := filter.NewCompareFilter(filter.Equal, comparator)
		rowFilter := filter.NewRowFilter(compareFilter)

		ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
		defer done()
		scanRequest, e := hrpc.NewScanRangeStr(ctx, meta.OBJECT_TABLE,
			startRowkey.String(), string(stopKey),
			// scan for max+1 rows to determine if results are truncated
			hrpc.Filters(rowFilter), hrpc.NumberOfRows(uint32(request.MaxKeys+1)))
		if e != nil {
			err = e
			return
		}
		scanResponse, e := yig.MetaStorage.Hbase.Scan(scanRequest)
		if e != nil {
			err = e
			return
		}
		if len(scanResponse) > 0 {
			if len(scanResponse) > request.MaxKeys {
				var lstObject *meta.Object
				lstObject, err = meta.ObjectFromResponse(scanResponse[request.MaxKeys])
				if err != nil {
					return
				}
				currMarker = lstObject.Name
				if request.Versioned {
					currVerMarkerNum = lstObject.GetVersionNumber()
				}

				scanResponse = scanResponse[0:request.MaxKeys+1]
				truncated = true
			} else {
				truncated = false
			}
		} else {
			truncated = false
			exit = true
		}
		// search objects
		var idx int
		var row *hrpc.Result
		for idx, row = range scanResponse {
			var o *meta.Object
			o, e = meta.ObjectFromResponse(row)
			if e != nil {
				err = e
				return
			}
			if _, ok := objectMap[o.Name]; !ok {
				objectMap[o.Name] = o
				if o.DeleteMarker && !request.Versioned {
					continue
				}
			} else {
				if !request.Versioned {
					skipOldVerObj = o.Name + meta.ObjectNameSmallestStr
					continue
				}
			}
			if count < request.MaxKeys {
				//request.Marker = o.Name
				nextMarker = o.Name
				if request.Versioned {
					nextVerIdMarker = o.VersionId
				}
			}

			if len(request.Delimiter) != 0 {
				objName := o.Name
				len := len(request.Prefix)
				subStr := objName[len:]
				n := strings.Index(subStr, request.Delimiter)
				if n != -1 {
					prefixKey := string([]rune(objName)[0:(len + n + 1)])
					if _, ok := commonPrefixes[prefixKey]; !ok {
						if count >= request.MaxKeys {
							truncated = true
							exit = true
							break
						}
						nextMarker = prefixKey
						commonPrefixes[prefixKey] = true

						skipAfterDelim = objName[0:(len + n)]
						skipAfterDelim += biggerThanDelim
						helper.Debugln("skipAfterDelim:", skipAfterDelim)
						count += 1
					}
					continue
				}
			}

			if count >= request.MaxKeys {
				truncated = true
				exit = true
				break
			}

			retObjects = append(retObjects, o)
			count += 1
		}
		if exit {
			break
		}
		truncated = truncated || (idx + 1 != len(scanResponse))
	}
	prefixes = helper.Keys(commonPrefixes)
	return
}

func (yig *YigStorage) ListObjects(credential iam.Credential, bucketName string,
	request datatype.ListObjectsRequest) (result meta.ListObjectsInfo, err error) {

	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
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

	retObjects, prefixes, truncated, nextMarker, _, err := yig.listObjects(credential, bucketName, request)
	if truncated && len(nextMarker) != 0 {
		result.NextMarker = nextMarker
	}
	if request.Version == 2 {
		result.NextMarker = meta.Encrypt(result.NextMarker)
	}
	objects := make([]datatype.Object, 0, len(retObjects))
	for _, obj := range retObjects {
		helper.Debugln("result:", obj.Name)
		object := datatype.Object{
			LastModified: obj.LastModifiedTime.UTC().Format(meta.CREATE_TIME_LAYOUT),
			ETag:         "\"" + obj.Etag + "\"",
			Size:         obj.Size,
			StorageClass: "STANDARD",
		}
		if request.EncodingType != "" {// only support "url" encoding for now
			object.Key = url.QueryEscape(obj.Name)
		} else {
			object.Key = obj.Name
		}

		if request.FetchOwner {
			var owner iam.Credential
			owner, err = iam.GetCredentialByUserId(obj.OwnerId)
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
	result.Prefixes = prefixes
	result.IsTruncated = truncated

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

	bucket, err := yig.MetaStorage.GetBucket(bucketName, true)
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

	retObjects, prefixes, truncated, nextMarker, nextVerIdMarker, err := yig.listObjects(credential, bucketName, request)
	if truncated && len(nextMarker) != 0 {
		result.NextKeyMarker = nextMarker
		result.NextVersionIdMarker = nextVerIdMarker
	}

	objects := make([]datatype.VersionedObject, 0, len(retObjects))
	for _, o := range retObjects {
		// TODO: IsLatest
		object := datatype.VersionedObject{
			LastModified: o.LastModifiedTime.UTC().Format(meta.CREATE_TIME_LAYOUT),
			ETag:         "\"" + o.Etag + "\"",
			Size:         o.Size,
			StorageClass: "STANDARD",
			Key:          o.Name,
		}
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
	result.Prefixes = prefixes
	result.IsTruncated = truncated

	if request.EncodingType != "" { // only support "url" encoding for now
		result.Prefixes = helper.Map(result.Prefixes, func(s string) string {
			return url.QueryEscape(s)
		})
		result.NextKeyMarker = url.QueryEscape(result.NextKeyMarker)
	}

	return
}
