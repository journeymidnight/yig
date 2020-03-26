package tidbclient

import (
	"database/sql"
	. "database/sql/driver"
	"encoding/json"
	"math"
	"strconv"
	"time"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) GetObject(bucketName, objectName, version string) (*Object, error) {
	var ibucketname, iname, customattributes, acl, lastModifiedTime string
	var iversion uint64
	var err error
	var row *sql.Row
	sqltext := "select bucketname,name,version,location,pool,ownerid,size,objectid,lastmodifiedtime,etag,contenttype," +
		"customattributes,acl,nullversion,deletemarker,ssetype,encryptionkey,initializationvector,type,storageclass,createtime from objects where bucketname=? and name=? "
	if version == NullVersion {
		sqltext += "and version=0"
		row = t.Client.QueryRow(sqltext, bucketName, objectName)
	} else {
		sqltext += "and version=?;"
		row = t.Client.QueryRow(sqltext, bucketName, objectName, version)
	}
	object := &Object{}
	err = row.Scan(
		&ibucketname,
		&iname,
		&object.VersionId,
		&object.Location,
		&object.Pool,
		&object.OwnerId,
		&object.Size,
		&object.ObjectId,
		&lastModifiedTime,
		&object.Etag,
		&object.ContentType,
		&customattributes,
		&acl,
		&object.NullVersion,
		&object.DeleteMarker,
		&object.SseType,
		&object.EncryptionKey,
		&object.InitializationVector,
		&object.Type,
		&object.StorageClass,
		&object.CreateTime,
	)
	if err == sql.ErrNoRows {
		err = ErrNoSuchKey
		return nil, ErrNoSuchKey
	} else if err != nil {
		return nil, err
	}
	object.LastModifiedTime, err = time.Parse("2006-01-02 15:04:05", lastModifiedTime)
	if err != nil {
		return nil, err
	}
	object.Name = objectName
	object.BucketName = bucketName
	err = json.Unmarshal([]byte(acl), &object.ACL)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(customattributes), &object.CustomAttributes)
	if err != nil {
		return nil, err
	}
	if object.Type == ObjectTypeMultipart {
		iversion = math.MaxUint64 - object.CreateTime
		object.Parts, err = getParts(object.BucketName, object.Name, iversion, t.Client)
		if err != nil {
			return nil, err
		}
		//build simple index for multipart
		if len(object.Parts) != 0 {
			var sortedPartNum = make([]int64, len(object.Parts))
			for k, v := range object.Parts {
				sortedPartNum[k-1] = v.Offset
			}
			object.PartsIndex = &SimpleIndex{Index: sortedPartNum}
		}
	}
	return object, nil
}

func (t *TidbClient) GetLatestObjectVersion(bucketName, objectName string) (*Object, error) {
	var customattributes, acl, lastModifiedTime string
	var err error
	var row *sql.Row
	var nullObjExists bool
	tx, err := t.Client.Begin()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		}
		if err != nil {
			tx.Rollback()
		}
	}()

	sqltext := "select bucketname,name,version,location,pool,ownerid,size,objectid,lastmodifiedtime,etag,contenttype," +
		"customattributes,acl,nullversion,deletemarker,ssetype,encryptionkey,initializationvector,type,storageclass,createtime from objects where bucketname=? and name=? and version=0"
	row = tx.QueryRow(sqltext, bucketName, objectName)
	nullObject := new(Object)
	err = row.Scan(
		&nullObject.BucketName,
		&nullObject.Name,
		&nullObject.VersionId,
		&nullObject.Location,
		&nullObject.Pool,
		&nullObject.OwnerId,
		&nullObject.Size,
		&nullObject.ObjectId,
		&lastModifiedTime,
		&nullObject.Etag,
		&nullObject.ContentType,
		&customattributes,
		&acl,
		&nullObject.NullVersion,
		&nullObject.DeleteMarker,
		&nullObject.SseType,
		&nullObject.EncryptionKey,
		&nullObject.InitializationVector,
		&nullObject.Type,
		&nullObject.StorageClass,
		&nullObject.CreateTime,
	)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if err != sql.ErrNoRows {
		nullObjExists = true
	}
	if nullObjExists {
		nullObject.LastModifiedTime, err = time.Parse("2006-01-02 15:04:05", lastModifiedTime)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal([]byte(acl), &nullObject.ACL)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal([]byte(customattributes), &nullObject.CustomAttributes)
		if err != nil {
			return nil, err
		}
		if nullObject.Type == ObjectTypeMultipart {
			partVersion := math.MaxUint64 - nullObject.CreateTime
			nullObject.Parts, err = getParts(nullObject.BucketName, nullObject.Name, partVersion, t.Client)
			if err != nil {
				return nil, err
			}
			//build simple index for multipart
			if len(nullObject.Parts) != 0 {
				var sortedPartNum = make([]int64, len(nullObject.Parts))
				for k, v := range nullObject.Parts {
					sortedPartNum[k-1] = v.Offset
				}
				nullObject.PartsIndex = &SimpleIndex{Index: sortedPartNum}
			}
		}
	}

	sqltext = "select bucketname,name,version,location,pool,ownerid,size,objectid,lastmodifiedtime,etag,contenttype," +
		"customattributes,acl,nullversion,deletemarker,ssetype,encryptionkey,initializationvector,type,storageclass,createtime " +
		"from objects where bucketname=? and name=? and version>0 order by version limit 1"
	rows, err := tx.Query(sqltext, bucketName, objectName)
	if err != nil {
		return nil, err
	}
	var object *Object
	for rows.Next() {
		object = &Object{}
		err = rows.Scan(
			&object.BucketName,
			&object.Name,
			&object.VersionId,
			&object.Location,
			&object.Pool,
			&object.OwnerId,
			&object.Size,
			&object.ObjectId,
			&lastModifiedTime,
			&object.Etag,
			&object.ContentType,
			&customattributes,
			&acl,
			&object.NullVersion,
			&object.DeleteMarker,
			&object.SseType,
			&object.EncryptionKey,
			&object.InitializationVector,
			&object.Type,
			&object.StorageClass,
			&object.CreateTime,
		)
		if err != nil {
			return nil, err
		}
		object.LastModifiedTime, err = time.Parse("2006-01-02 15:04:05", lastModifiedTime)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal([]byte(acl), &object.ACL)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal([]byte(customattributes), &object.CustomAttributes)
		if err != nil {
			return nil, err
		}
		if object.Type == ObjectTypeMultipart {
			partVersion := math.MaxUint64 - object.CreateTime
			object.Parts, err = getParts(object.BucketName, object.Name, partVersion, t.Client)
			if err != nil {
				return nil, err
			}
			//build simple index for multipart
			if len(object.Parts) != 0 {
				var sortedPartNum = make([]int64, len(object.Parts))
				for k, v := range object.Parts {
					sortedPartNum[k-1] = v.Offset
				}
				object.PartsIndex = &SimpleIndex{Index: sortedPartNum}
			}
		}
	}
	if !nullObjExists && object == nil {
		return nil, ErrNoSuchKey
	} else if !nullObjExists {
		return object, nil
	} else if object == nil {
		return nullObject, nil
	} else {
		retObject := helper.Ternary(nullObject.LastModifiedTime.After(object.LastModifiedTime), nullObject, object)
		return retObject.(*Object), nil
	}

	return object, nil

}

func (t *TidbClient) UpdateObjectAttrs(object *Object) error {
	sql, args := object.GetUpdateAttrsSql()
	_, err := t.Client.Exec(sql, args...)
	return err
}

func (t *TidbClient) UpdateObjectAcl(object *Object) error {
	sql, args := object.GetUpdateAclSql()
	_, err := t.Client.Exec(sql, args...)
	return err
}

func (t *TidbClient) RenameObject(object *Object, sourceObject string) (err error) {
	sql, args := object.GetUpdateNameSql(sourceObject)
	if len(object.Parts) != 0 {
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
		_, err = tx.Exec(sql, args...)
		if err != nil {
			return err
		}

		// rename parts
		sql, args = object.GetUpdateObjectPartNameSql(sourceObject)
		_, err = tx.Exec(sql, args...)
		return err
	}
	_, err = t.Client.Exec(sql, args...)
	return
}

func (t *TidbClient) ReplaceObjectMetas(object *Object, tx Tx) (err error) {
	sql, args := object.GetReplaceObjectMetasSql()
	_, err = t.Client.Exec(sql, args...)
	return
}

func (t *TidbClient) UpdateAppendObject(object *Object) (err error) {
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

	lastModifiedTime := object.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := "update objects set lastmodifiedtime=?, size=?, createtime=? where bucketname=? and name=? and version=?"
	args := []interface{}{lastModifiedTime, object.Size, object.LastModifiedTime.UnixNano(), object.BucketName, object.Name, object.VersionId}
	_, err = tx.Exec(sql, args...)

	return t.UpdateUsage(object.BucketName, object.Size, tx)
}

func (t *TidbClient) PutObject(object *Object, multipart *Multipart, updateUsage bool) (err error) {
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

	sql, args := object.GetCreateSql()
	_, err = tx.Exec(sql, args...)
	if object.Parts != nil {
		v := math.MaxUint64 - uint64(object.LastModifiedTime.UnixNano())
		version := strconv.FormatUint(v, 10)
		for _, p := range object.Parts {
			psql, args := p.GetCreateSql(object.BucketName, object.Name, version)
			_, err = tx.Exec(psql, args...)
			if err != nil {
				return err
			}
		}
	}

	if multipart != nil {
		return t.DeleteMultipart(multipart, tx)
	}

	if updateUsage {
		return t.UpdateUsage(object.BucketName, object.Size, tx)
	}

	return nil
}

func (t *TidbClient) UpdateObject(object *Object, multipart *Multipart, updateUsage bool, tx Tx) (err error) {
	if tx == nil {
		tx, err = t.Client.Begin()
		if err != nil {
			return err
		}
		defer func() {
			if err == nil {
				err = tx.(*sql.Tx).Commit()
			}
			if err != nil {
				tx.(*sql.Tx).Rollback()
			}
		}()
	}
	txn := tx.(*sql.Tx)

	sql, args := object.GetUpdateSql()
	_, err = txn.Exec(sql, args...)
	if object.Parts != nil {
		v := math.MaxUint64 - uint64(object.LastModifiedTime.UnixNano())
		version := strconv.FormatUint(v, 10)
		for _, p := range object.Parts {
			psql, args := p.GetCreateSql(object.BucketName, object.Name, version)
			_, err = txn.Exec(psql, args...)
			if err != nil {
				return err
			}
		}
	}

	if multipart != nil {
		err = t.DeleteMultipart(multipart, tx)
		if err != nil {
			return err
		}
	}

	if updateUsage {
		err = t.UpdateUsage(object.BucketName, object.Size, tx)
		if err != nil {
			return err
		}
	}

	return err
}

func (t *TidbClient) UpdateFreezerObject(object *Object, tx Tx) (err error) {
	if tx == nil {
		tx, err = t.Client.Begin()
		if err != nil {
			return err
		}
		defer func() {
			if err == nil {
				err = tx.(*sql.Tx).Commit()
			}
			if err != nil {
				tx.(*sql.Tx).Rollback()
			}
		}()
	}
	txn := tx.(*sql.Tx)
	v := math.MaxUint64 - uint64(object.LastModifiedTime.UnixNano())
	version := strconv.FormatUint(v, 10)
	sqltext := "delete from objectpart where objectname=? and bucketname=? and version=?;"
	_, err = txn.Exec(sqltext, object.Name, object.BucketName, version)
	if err != nil {
		return err
	}

	sql, args := object.GetGlacierUpdateSql()
	_, err = txn.Exec(sql, args...)
	if object.Parts != nil {
		for _, p := range object.Parts {
			psql, args := p.GetCreateSql(object.BucketName, object.Name, version)
			_, err = txn.Exec(psql, args...)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *TidbClient) DeleteObject(object *Object, tx Tx) (err error) {
	if tx == nil {
		tx, err = t.Client.Begin()
		if err != nil {
			return err
		}
		defer func() {
			if err == nil {
				err = tx.(*sql.Tx).Commit()
			}
			if err != nil {
				tx.(*sql.Tx).Rollback()
			}
		}()
	}

	sqltext := "delete from objects where name=? and bucketname=? and version=?;"
	_, err = tx.(*sql.Tx).Exec(sqltext, object.Name, object.BucketName, object.VersionId)
	if err != nil {
		return err
	}

	return t.DeleteObjectPart(object, tx)
}

func (t *TidbClient) DeleteObjectPart(object *Object, tx Tx) (err error) {
	if object.Parts == nil {
		return nil
	}
	if tx == nil {
		tx, err = t.Client.Begin()
		if err != nil {
			return err
		}
		defer func() {
			if err == nil {
				err = tx.(*sql.Tx).Commit()
			}
			if err != nil {
				tx.(*sql.Tx).Rollback()
			}
		}()
	}

	partVersion := strconv.FormatUint(math.MaxUint64-object.CreateTime, 10)
	sqltext := "delete from objectpart where objectname=? and bucketname=? and version=?;"
	_, err = tx.(*sql.Tx).Exec(sqltext, object.Name, object.BucketName, partVersion)
	if err != nil {
		return err
	}
	return nil
}

//util function
func getParts(bucketName, objectName string, version uint64, cli *sql.DB) (parts map[int]*Part, err error) {
	parts = make(map[int]*Part)
	sqltext := "select partnumber,size,objectid,offset,etag,lastmodified,initializationvector from objectpart where bucketname=? and objectname=? and version=?;"
	rows, err := cli.Query(sqltext, bucketName, objectName, version)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var p *Part = &Part{}
		err = rows.Scan(
			&p.PartNumber,
			&p.Size,
			&p.ObjectId,
			&p.Offset,
			&p.Etag,
			&p.LastModified,
			&p.InitializationVector,
		)
		parts[p.PartNumber] = p
	}
	return
}
