package tidbclient

import (
	"database/sql"
	. "database/sql/driver"
	"encoding/json"
	"strings"
	"time"

	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) GetBucket(bucketName string) (bucket *Bucket, err error) {
	var acl, cors, logging, lc, policy, website, encryption, createTime string
	sqltext := "select bucketname,acl,cors,COALESCE(logging,\"\"),lc,uid,policy,website,COALESCE(encryption,\"\"),createtime,usages,versioning from buckets where bucketname=?;"
	bucket = new(Bucket)
	err = t.Client.QueryRow(sqltext, bucketName).Scan(
		&bucket.Name,
		&acl,
		&cors,
		&logging,
		&lc,
		&bucket.OwnerId,
		&policy,
		&website,
		&encryption,
		&createTime,
		&bucket.Usage,
		&bucket.Versioning,
	)
	if err != nil && err == sql.ErrNoRows {
		err = ErrNoSuchBucket
		return
	} else if err != nil {
		return
	}
	bucket.CreateTime, err = time.Parse(TIME_LAYOUT_TIDB, createTime)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(acl), &bucket.ACL)
	if err != nil {
		helper.Logger.Error("Unable to unmarshal acl:", acl)
		return
	}
	err = json.Unmarshal([]byte(cors), &bucket.CORS)
	if err != nil {
		helper.Logger.Error("Unable to unmarshal cors:", cors)
		return
	}
	err = json.Unmarshal([]byte(logging), &bucket.BucketLogging)
	if err != nil {
		helper.Logger.Error("Unable to unmarshal logging:", logging)
		return
	}
	err = json.Unmarshal([]byte(lc), &bucket.Lifecycle)
	if err != nil {
		helper.Logger.Error("Unable to unmarshal lc:", lc)
		return
	}
	bucket.Policy = []byte(policy)
	err = json.Unmarshal([]byte(website), &bucket.Website)
	if err != nil {
		helper.Logger.Error("Unable to unmarshal website:", website)
		return
	}
	err = json.Unmarshal([]byte(encryption), &bucket.Encryption)
	if err != nil {
		helper.Logger.Error("Unable to unmarshal encryption:", encryption)
		return
	}
	return
}

func (t *TidbClient) GetBuckets() (buckets []Bucket, err error) {
	sqltext := "select bucketname,acl,cors,COALESCE(logging,\"\"),lc,uid,policy,website,COALESCE(encryption,\"\"),createtime,usages,versioning from buckets;"
	rows, err := t.Client.Query(sqltext)
	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var tmp Bucket
		var acl, cors, logging, lc, website, encryption, createTime string
		err = rows.Scan(
			&tmp.Name,
			&acl,
			&cors,
			&logging,
			&lc,
			&tmp.OwnerId,
			&tmp.Policy,
			&website,
			&encryption,
			&createTime,
			&tmp.Usage,
			&tmp.Versioning)
		if err != nil {
			return
		}
		tmp.CreateTime, err = time.Parse(TIME_LAYOUT_TIDB, createTime)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(acl), &tmp.ACL)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(cors), &tmp.CORS)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(logging), &tmp.BucketLogging)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(lc), &tmp.Lifecycle)
		if err != nil {
			return
		}

		err = json.Unmarshal([]byte(website), &tmp.Website)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(encryption), &tmp.Encryption)
		if err != nil {
			return
		}
		buckets = append(buckets, tmp)
	}
	return
}

//Actually this method is used to update bucket
func (t *TidbClient) PutBucket(bucket Bucket) error {
	sql, args := bucket.GetUpdateSql()
	_, err := t.Client.Exec(sql, args...)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) PutNewBucket(bucket Bucket) error {
	tx, err := t.Client.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		}
		if err != nil {
			tx.Rollback()
		}
	}()
	sql, args := bucket.GetCreateSql()
	_, err = tx.Exec(sql, args...)
	if err != nil {
		return err
	}
	user_sql := "insert into users(userid,bucketname) values(?,?)"
	_, err = t.Client.Exec(user_sql, bucket.OwnerId, bucket.Name)
	return err
}

func (t *TidbClient) CheckAndPutBucket(bucket Bucket) (bool, error) {
	var processed bool
	_, err := t.GetBucket(bucket.Name)
	if err == nil {
		processed = false
		return processed, err
	} else if err != nil && err != ErrNoSuchBucket {
		processed = false
		return processed, err
	} else {
		processed = true
	}
	sql, args := bucket.GetCreateSql()
	_, err = t.Client.Exec(sql, args...)
	return processed, err
}

func (t *TidbClient) ListObjects(bucketName, marker, prefix, delimiter string, maxKeys int) (listInfo ListObjectsInfo, err error) {
	var count int
	var exit bool
	objectMap := make(map[string]struct{})
	objectNum := make(map[string]int)
	commonPrefixes := make(map[string]struct{})
	omarker := marker
	for {
		var loopcount int
		var sqltext string
		var rows *sql.Rows
		if marker == "" {
			sqltext = "select bucketname,name,version,nullversion,deletemarker,ownerid,etag,lastmodifiedtime,storageclass,size" +
				" from objects where bucketName=? order by bucketname,name,version limit ?;"
			rows, err = t.Client.Query(sqltext, bucketName, maxKeys)
		} else {
			sqltext = "select bucketname,name,version,nullversion,deletemarker,ownerid,etag,lastmodifiedtime,storageclass,size" +
				" from objects where bucketName=? and name >=? order by bucketname,name,version limit ?,?;"

			rows, err = t.Client.Query(sqltext, bucketName, marker, objectNum[marker], objectNum[marker]+maxKeys)
		}
		if err != nil {
			return
		}
		defer rows.Close()
		for rows.Next() {
			loopcount += 1
			//fetch related date
			var bucketname, name, ownerid string
			var version, etag, lastModified string
			var nullversion, deletemarker bool
			var size int64
			var storageClassType StorageClass
			err = rows.Scan(
				&bucketname,
				&name,
				&version,
				&nullversion,
				&deletemarker,
				&ownerid,
				&etag,
				&lastModified,
				&storageClassType,
				&size,
			)
			if err != nil {
				return
			}
			//prepare next marker
			//TODU: be sure how tidb/mysql compare strings
			if _, ok := objectNum[name]; !ok {
				objectNum[name] = 0
			}
			objectNum[name] += 1
			marker = name
			//filte row
			//filte by prefix
			hasPrefix := strings.HasPrefix(name, prefix)
			if !hasPrefix {
				continue
			}
			//filte by objectname
			if _, ok := objectMap[name]; !ok {
				objectMap[name] = struct{}{}
			} else {
				continue
			}
			//filte by deletemarker
			if deletemarker {
				continue
			}
			if name == omarker {
				continue
			}
			//filte by delemiter
			if len(delimiter) != 0 {
				subStr := strings.TrimPrefix(name, prefix)
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := prefix + string([]byte(subStr)[0:(n+1)])
					if prefixKey == omarker {
						continue
					}
					if _, ok := commonPrefixes[prefixKey]; !ok {
						if count == maxKeys {
							listInfo.IsTruncated = true
							exit = true
							break
						}
						commonPrefixes[prefixKey] = struct{}{}
						listInfo.NextMarker = prefixKey
						count += 1
					}
					continue
				}
			}
			var o datatype.Object
			o.Key = name
			o.Owner = datatype.Owner{ID: ownerid}
			o.ETag = etag
			lastt, err := time.Parse(TIME_LAYOUT_TIDB, lastModified)
			if err != nil {
				return listInfo, err
			}
			o.LastModified = lastt.UTC().Format(CREATE_TIME_LAYOUT)
			o.Size = size
			o.StorageClass = storageClassType.ToString()

			count += 1
			if count == maxKeys {
				listInfo.NextMarker = name
			}
			if count == 0 {
				continue
			}
			if count > maxKeys {
				listInfo.IsTruncated = true
				exit = true
				break
			}
			listInfo.Objects = append(listInfo.Objects, o)
		}
		if loopcount == 0 {
			exit = true
		}
		if exit {
			break
		}
	}
	listInfo.Prefixes = helper.Keys(commonPrefixes)
	return
}

func modifyMetaToObjectResult(objMeta Object) datatype.Object {
	var o datatype.Object
	o.Key = objMeta.Name
	o.Owner = datatype.Owner{ID: objMeta.OwnerId}
	o.ETag = objMeta.Etag
	o.LastModified = objMeta.LastModifiedTime.Format(CREATE_TIME_LAYOUT)
	o.Size = objMeta.Size
	o.StorageClass = objMeta.StorageClass.ToString()
	return o
}

func modifyMetaToVersionedObjectResult(objMeta Object) datatype.VersionedObject {
	var o datatype.VersionedObject
	o.Key = objMeta.Name
	o.Owner = datatype.Owner{ID: objMeta.OwnerId}
	o.ETag = objMeta.Etag
	o.LastModified = objMeta.LastModifiedTime.Format(CREATE_TIME_LAYOUT)
	o.Size = objMeta.Size
	o.StorageClass = objMeta.StorageClass.ToString()
	if objMeta.VersionId == NullVersion {
		objMeta.VersionId = "null"
	}
	o.VersionId = objMeta.VersionId
	o.DeleteMarker = objMeta.DeleteMarker
	return o
}

func (t *TidbClient) ListLatestObjects(bucketName, marker, prefix, delimiter string, maxKeys int) (listInfo ListObjectsInfo, err error) {
	var count int
	var exit bool
	objectMap := make(map[string]interface{})
	commonPrefixes := make(map[string]interface{})
	currentMarker := marker
	var lastModifiedTime string
	for {
		var sqltext string
		var rows *sql.Rows
		if currentMarker == "" {
			sqltext = "select bucketname,name,version,deletemarker,ownerid,etag,lastmodifiedtime,storageclass,size,createtime" +
				" from objects where bucketName=? order by bucketname,name,version limit ?;"
			rows, err = t.Client.Query(sqltext, bucketName, maxKeys)
		} else {
			sqltext = "select bucketname,name,version,deletemarker,ownerid,etag,lastmodifiedtime,storageclass,size,createtime" +
				" from objects where bucketName=? and name>? order by bucketname,name,version limit ?;"
			rows, err = t.Client.Query(sqltext, bucketName, currentMarker, maxKeys)
		}
		if err != nil {
			return
		}
		defer rows.Close()

		var loopCount int
		var previousNullObjectMeta *Object
		for rows.Next() {
			loopCount += 1
			//fetch related date
			objMeta := Object{}
			err = rows.Scan(
				&objMeta.BucketName,
				&objMeta.Name,
				&objMeta.VersionId,
				&objMeta.DeleteMarker,
				&objMeta.OwnerId,
				&objMeta.Etag,
				&lastModifiedTime,
				&objMeta.StorageClass,
				&objMeta.Size,
				&objMeta.CreateTime,
			)
			if err != nil {
				return
			}
			currentMarker = objMeta.Name

			objMeta.LastModifiedTime, _ = time.Parse(TIME_LAYOUT_TIDB, lastModifiedTime)
			// Compare which is the latest of null version object and versioned object
			if previousNullObjectMeta != nil {
				var meta Object
				if objMeta.Name != previousNullObjectMeta.Name {
					meta = *previousNullObjectMeta
				} else {
					if objMeta.CreateTime > previousNullObjectMeta.CreateTime {
						meta = objMeta
					} else {
						meta = *previousNullObjectMeta
					}
				}

				if meta.DeleteMarker {
					objectMap[meta.Name] = nil
					continue
				}

				o := modifyMetaToObjectResult(meta)

				count++
				if count == maxKeys {
					listInfo.NextMarker = o.Key
				}

				if count > maxKeys {
					listInfo.IsTruncated = true
					exit = true
					break
				}
				objectMap[meta.Name] = nil
				listInfo.Objects = append(listInfo.Objects, o)

				// Compare once

				if objMeta.Name == previousNullObjectMeta.Name {
					previousNullObjectMeta = nil
					continue
				}
				previousNullObjectMeta = nil
			}

			// If object key has in result of CommonPrefix or Objects, do continue
			if _, ok := objectMap[objMeta.Name]; ok {
				continue
			}

			if !strings.HasPrefix(objMeta.Name, prefix) {
				continue
			}

			// If delete marker, do continue
			if objMeta.DeleteMarker {
				continue
			}

			//filter prefix by delimiter
			if delimiter != "" {
				subKey := strings.TrimPrefix(objMeta.Name, prefix)
				sp := strings.SplitN(subKey, delimiter, 2)
				if len(sp) == 2 {
					prefixKey := prefix + sp[0] + delimiter
					if _, ok := commonPrefixes[prefixKey]; !ok && prefixKey != marker {
						count++
						if count == maxKeys {
							listInfo.NextMarker = prefixKey
						}
						if count > maxKeys {
							listInfo.IsTruncated = true
							exit = true
							break
						}
						commonPrefixes[prefixKey] = nil
					}
					continue
				}
			}

			if objMeta.VersionId == NullVersion {
				previousNullObjectMeta = &objMeta
				continue
			} else {
				previousNullObjectMeta = nil
			}

			var o = modifyMetaToObjectResult(objMeta)

			count++
			if count == maxKeys {
				listInfo.NextMarker = objMeta.Name
			}

			if count > maxKeys {
				listInfo.IsTruncated = true
				exit = true
				break
			}
			objectMap[objMeta.Name] = nil
			listInfo.Objects = append(listInfo.Objects, o)
		}

		// If the last one result is a null version
		if previousNullObjectMeta != nil {

			sqltext = "select bucketname,name,version,deletemarker,ownerid,etag,lastmodifiedtime,storageclass,size,createtime" +
				" from objects where bucketName=? and name=? and version>0 order by bucketname,name,version limit 1;"
			row := t.Client.QueryRow(sqltext, bucketName, previousNullObjectMeta.Name)
			objMeta := Object{}
			err = row.Scan(
				&objMeta.BucketName,
				&objMeta.Name,
				&objMeta.VersionId,
				&objMeta.DeleteMarker,
				&objMeta.OwnerId,
				&objMeta.Etag,
				&lastModifiedTime,
				&objMeta.StorageClass,
				&objMeta.Size,
				&objMeta.CreateTime,
			)
			if err != nil && err != sql.ErrNoRows {
				return
			}
			objMeta.LastModifiedTime, _ = time.Parse(TIME_LAYOUT_TIDB, lastModifiedTime)
			var meta Object
			if err == sql.ErrNoRows {
				meta = *previousNullObjectMeta
			} else if objMeta.CreateTime > previousNullObjectMeta.CreateTime {
				meta = objMeta
			} else {
				meta = *previousNullObjectMeta
			}

			if meta.DeleteMarker {
				objectMap[meta.Name] = nil
				continue
			}

			o := modifyMetaToObjectResult(meta)
			count++
			if count == maxKeys {
				listInfo.NextMarker = o.Key
			}

			if count > maxKeys {
				listInfo.IsTruncated = true
				exit = true
				break
			}
			objectMap[meta.Name] = nil
			listInfo.Objects = append(listInfo.Objects, o)

			previousNullObjectMeta = nil
		}

		if loopCount == 0 {
			exit = true
		}
		if exit {
			break
		}
	}
	// fill CommonPrefix
	listInfo.Prefixes = helper.Keys(commonPrefixes)
	return
}

func (t *TidbClient) ListVersionedObjects(bucketName, marker, verIdMarker, prefix, delimiter string, maxKeys int) (listInfo VersionedListObjectsInfo, err error) {
	var count int
	var exit bool
	commonPrefixes := make(map[string]interface{})
	currentKeyMarker := marker
	currentVerIdMarker := verIdMarker
	var previousNullObjectMeta *Object
	var lastModifiedTime string
	// Handle marker data first
	if currentKeyMarker != "" {
		var needCompareNull = true
		nullObjMeta := Object{}
		// Find null version first with specified marker
		sqltext := "select bucketname,name,version,deletemarker,ownerid,etag,lastmodifiedtime,storageclass,size,createtime" +
			" from objects where bucketName=? and name=? and version=0;"
		row := t.Client.QueryRow(sqltext, bucketName, maxKeys)
		err = row.Scan(
			&nullObjMeta.BucketName,
			&nullObjMeta.Name,
			&nullObjMeta.VersionId,
			&nullObjMeta.DeleteMarker,
			&nullObjMeta.OwnerId,
			&nullObjMeta.Etag,
			&lastModifiedTime,
			&nullObjMeta.StorageClass,
			&nullObjMeta.Size,
			&nullObjMeta.CreateTime,
		)
		if err != nil && err != sql.ErrNoRows {
			return listInfo, err
		}
		nullObjMeta.LastModifiedTime, _ = time.Parse(TIME_LAYOUT_TIDB, lastModifiedTime)
		if err == sql.ErrNoRows && currentVerIdMarker == "null" {
			return listInfo, nil
		}
		// Calculate the null object version to compare with other versioned object
		nullVerIdMarker := nullObjMeta.GenVersionId(datatype.BucketVersioningEnabled)
		if currentVerIdMarker == "null" {
			needCompareNull = false
			currentVerIdMarker = nullVerIdMarker
		} else if nullVerIdMarker < currentVerIdMarker {
			// currentVerIdMarker is older than null object
			needCompareNull = false
		}

		for {
			var rows *sql.Rows
			var o datatype.VersionedObject
			sqltext = "select bucketname,name,version,deletemarker,ownerid,etag,lastmodifiedtime,storageclass,size,createtime" +
				" from objects where bucketName=? and name=? and version>? order by bucketname,name,version limit ?;"
			rows, err = t.Client.Query(sqltext, bucketName, currentKeyMarker, currentVerIdMarker, maxKeys)
			if err != nil {
				return
			}
			defer rows.Close()
			for rows.Next() {
				VerObjMeta := Object{}
				err = rows.Scan(
					&VerObjMeta.BucketName,
					&VerObjMeta.Name,
					&VerObjMeta.VersionId,
					&VerObjMeta.DeleteMarker,
					&VerObjMeta.OwnerId,
					&VerObjMeta.Etag,
					&lastModifiedTime,
					&VerObjMeta.StorageClass,
					&VerObjMeta.Size,
					&VerObjMeta.CreateTime,
				)
				if err != nil {
					return
				}
				currentKeyMarker = VerObjMeta.Name
				currentVerIdMarker = VerObjMeta.VersionId
				VerObjMeta.LastModifiedTime, _ = time.Parse(TIME_LAYOUT_TIDB, lastModifiedTime)
				if needCompareNull {
					if nullObjMeta.CreateTime > VerObjMeta.CreateTime {
						needCompareNull = false
						currentVerIdMarker = nullObjMeta.VersionId
						o = modifyMetaToVersionedObjectResult(nullObjMeta)

					} else {
						o = modifyMetaToVersionedObjectResult(VerObjMeta)
					}
					count++
					if count == maxKeys {
						listInfo.NextKeyMarker = o.Key
						listInfo.NextVersionIdMarker = o.VersionId
					}
					if count > maxKeys {
						listInfo.IsTruncated = true
						exit = true
						break
					}
					listInfo.Objects = append(listInfo.Objects, o)
				}
			}
		}
	}

	if exit {
		return listInfo, nil
	}
	// clear verion marker
	currentVerIdMarker = ""

	// Begin to list other objects
	for {
		var sqltext string
		var rows *sql.Rows
		if currentKeyMarker == "" {
			sqltext = "select bucketname,name,version,deletemarker,ownerid,etag,lastmodifiedtime,storageclass,size,createtime" +
				" from objects where bucketName=? order by bucketname,name,version limit ?;"
			rows, err = t.Client.Query(sqltext, bucketName, maxKeys)
		} else if currentVerIdMarker == "" {
			sqltext = "select bucketname,name,version,deletemarker,ownerid,etag,lastmodifiedtime,storageclass,size,createtime" +
				" from objects where bucketName=? and name>? order by bucketname,name,version limit ?;"
			rows, err = t.Client.Query(sqltext, bucketName, currentKeyMarker, maxKeys)
		} else {
			sqltext = "select bucketname,name,version,deletemarker,ownerid,etag,lastmodifiedtime,storageclass,size,createtime" +
				" from objects where bucketName=? and name>=? and version>? order by bucketname,name,version limit ?;"
			rows, err = t.Client.Query(sqltext, bucketName, currentKeyMarker, currentVerIdMarker, maxKeys)
		}
		if err != nil {
			return
		}
		defer rows.Close()

		var loopCount int
		for rows.Next() {
			loopCount += 1
			//fetch related date
			objMeta := Object{}
			err = rows.Scan(
				&objMeta.BucketName,
				&objMeta.Name,
				&objMeta.VersionId,
				&objMeta.DeleteMarker,
				&objMeta.OwnerId,
				&objMeta.Etag,
				&lastModifiedTime,
				&objMeta.StorageClass,
				&objMeta.Size,
				&objMeta.CreateTime,
			)
			if err != nil {
				return
			}

			currentKeyMarker = objMeta.Name
			currentVerIdMarker = objMeta.VersionId
			objMeta.LastModifiedTime, _ = time.Parse(TIME_LAYOUT_TIDB, lastModifiedTime)

			if previousNullObjectMeta != nil {
				if objMeta.Name != previousNullObjectMeta.Name {
					// fill in previous NullObject
					count++
					if count == maxKeys {
						listInfo.NextKeyMarker = previousNullObjectMeta.Name
						listInfo.NextVersionIdMarker = previousNullObjectMeta.VersionId
					}

					if count > maxKeys {
						listInfo.IsTruncated = true
						exit = true
						break
					}

					o := modifyMetaToVersionedObjectResult(*previousNullObjectMeta)
					listInfo.Objects = append(listInfo.Objects, o)
					previousNullObjectMeta = nil
				} else {
					// Compare which is the latest of null version object and versioned object
					var o datatype.VersionedObject
					nullIsNewer := previousNullObjectMeta.CreateTime > objMeta.CreateTime
					if nullIsNewer {
						o = modifyMetaToVersionedObjectResult(*previousNullObjectMeta)
						previousNullObjectMeta = nil
					} else {
						o = modifyMetaToVersionedObjectResult(objMeta)
					}

					count++
					if count == maxKeys {
						listInfo.NextKeyMarker = o.Key
						listInfo.NextVersionIdMarker = o.VersionId
					}

					if count > maxKeys {
						listInfo.IsTruncated = true
						exit = true
						break
					}

					listInfo.Objects = append(listInfo.Objects, o)

					if !nullIsNewer {
						continue
					}
				}
			}

			if !strings.HasPrefix(objMeta.Name, prefix) {
				continue
			}

			//filter prefix by delimiter
			if delimiter != "" {
				subKey := strings.TrimPrefix(objMeta.Name, prefix)
				sp := strings.SplitN(subKey, delimiter, 2)
				if len(sp) == 2 {
					prefixKey := prefix + sp[0] + delimiter
					if _, ok := commonPrefixes[prefixKey]; !ok && prefixKey != currentKeyMarker {
						count++
						if count == maxKeys {
							listInfo.NextKeyMarker = prefixKey
							listInfo.NextVersionIdMarker = objMeta.VersionId
						}
						if count > maxKeys {
							listInfo.IsTruncated = true
							exit = true
							break
						}
						commonPrefixes[prefixKey] = nil
					}
					continue
				}
			}

			if objMeta.VersionId == NullVersion {
				previousNullObjectMeta = &objMeta
				continue
			}

			o := modifyMetaToVersionedObjectResult(objMeta)

			count++
			if count == maxKeys {
				listInfo.NextKeyMarker = o.Key
				listInfo.NextVersionIdMarker = o.VersionId
			}

			if count > maxKeys {
				listInfo.IsTruncated = true
				exit = true
				break
			}
			listInfo.Objects = append(listInfo.Objects, o)

		}
		if exit {
			break
		}

		//  The last one result is a null version object and name is not same as the previous object

		if loopCount == 0 {
			if previousNullObjectMeta != nil {
				o := modifyMetaToVersionedObjectResult(*previousNullObjectMeta)

				count++
				if count == maxKeys {
					listInfo.NextKeyMarker = o.Key
					listInfo.NextVersionIdMarker = o.VersionId
				}

				if count > maxKeys {
					listInfo.IsTruncated = true
					exit = true
					break
				}
				listInfo.Objects = append(listInfo.Objects, o)
			}
			exit = true
		}
		if exit {
			break
		}

	}
	// fill CommonPrefix
	listInfo.Prefixes = helper.Keys(commonPrefixes)
	return
}

func (t *TidbClient) DeleteBucket(bucket Bucket) error {
	tx, err := t.Client.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		}
		if err != nil {
			tx.Rollback()
		}
	}()
	sql_delete_bucket := "delete from buckets where bucketname=?;"
	_, err = tx.Exec(sql_delete_bucket, bucket.Name)
	if err != nil {
		return err
	}

	sql_delete_user := "delete from users where userid=? and bucketname=?;"
	_, err = tx.Exec(sql_delete_user, bucket.OwnerId, bucket.Name)
	if err != nil {
		return err
	}

	sql_delete_lifecycle := "delete from lifecycle where bucketname=?;"
	_, err = tx.Exec(sql_delete_lifecycle, bucket.Name)
	if err != nil {
		return err
	}
	return nil
}

//TODO: Only find one object
func (t *TidbClient) IsEmptyBucket(bucketName string) (bool, error) {
	listInfo, err := t.ListObjects(bucketName, "", "", "", 1)
	if err != nil {
		return false, err
	}
	if len(listInfo.Objects) != 0 {
		return false, nil
	}
	// Check if object part is empty
	result, err := t.ListMultipartUploads(bucketName, "", "", "", "", "", 1)
	if err != nil {
		return false, err
	}
	if len(result.Uploads) != 0 {
		return false, nil
	}
	return true, nil
}

func (t *TidbClient) UpdateUsage(bucketName string, size int64, tx Tx) (err error) {
	if !helper.CONFIG.PiggybackUpdateUsage {
		return nil
	}
	sqlStr := "update buckets set usages= usages + ? where bucketname=?;"
	if tx == nil {
		_, err = t.Client.Exec(sqlStr, size, bucketName)
		return err
	}
	_, err = tx.(*sql.Tx).Exec(sqlStr, size, bucketName)
	return err
}
