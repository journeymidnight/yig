package tidbclient

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"math"
	"strconv"
	"time"

	. "github.com/journeymidnight/yig/error"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/xxtea/xxtea-go/xxtea"
)

func (t *TidbClient) GetObject(bucketName, objectName, version string) (object *Object, err error) {
	var ibucketname, iname, customattributes, acl, lastModifiedTime string
	var iversion uint64

	var row *sql.Row
	sqltext := "select bucketname,name,version,location,pool,ownerid,size,objectid,lastmodifiedtime,etag,contenttype," +
		"customattributes,acl,nullversion,deletemarker,ssetype,encryptionkey,initializationvector,type,storageclass from objects where bucketname=? and name=? "
	if version == "" {
		sqltext += "order by bucketname,name,version limit 1;"
		row = t.Client.QueryRow(sqltext, bucketName, objectName)
	} else {
		sqltext += "and version=?;"
		row = t.Client.QueryRow(sqltext, bucketName, objectName, version)
	}
	object = &Object{}
	err = row.Scan(
		&ibucketname,
		&iname,
		&iversion,
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
	)
	if err == sql.ErrNoRows {
		err = ErrNoSuchKey
		return
	} else if err != nil {
		return
	}
	rversion := math.MaxUint64 - iversion
	s := int64(rversion) / 1e9
	ns := int64(rversion) % 1e9
	object.LastModifiedTime = time.Unix(s, ns)
	object.Name = objectName
	object.BucketName = bucketName
	err = json.Unmarshal([]byte(acl), &object.ACL)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(customattributes), &object.CustomAttributes)
	if err != nil {
		return
	}
	object.Parts, err = getParts(object.BucketName, object.Name, iversion, t.Client)
	//build simple index for multipart
	if len(object.Parts) != 0 {
		var sortedPartNum = make([]int64, len(object.Parts))
		for k, v := range object.Parts {
			sortedPartNum[k-1] = v.Offset
		}
		object.PartsIndex = &SimpleIndex{Index: sortedPartNum}
	}
	var reversedTime uint64
	timestamp := math.MaxUint64 - reversedTime
	timeData := []byte(strconv.FormatUint(timestamp, 10))
	object.VersionId = hex.EncodeToString(xxtea.Encrypt(timeData, XXTEA_KEY))
	return
}

func (t *TidbClient) GetAllObject(bucketName, objectName, version string) (object []*Object, err error) {
	sqltext := "select version from objects where bucketname=? and name=?;"
	var versions []string
	rows, err := t.Client.Query(sqltext, bucketName, objectName)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var sversion string
		err = rows.Scan(&sversion)
		if err != nil {
			return
		}
		versions = append(versions, sversion)
	}
	for _, v := range versions {
		var obj *Object
		obj, err = t.GetObject(bucketName, objectName, v)
		if err != nil {
			return
		}
		object = append(object, obj)
	}
	return
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

func (t *TidbClient) RenameObject(object *Object, sourceObject string, tx DB) (err error) {
	if tx == nil {
		tx = t.Client
	}
	sql, args := object.GetUpdateNameSql(sourceObject)
	_, err = tx.Exec(sql, args...)
	return
}

func (t *TidbClient) ReplaceObjectMetas(object *Object, tx DB) (err error) {
	if tx == nil {
		tx = t.Client
	}
	sql, args := object.GetReplaceObjectMetasSql()
	_, err = tx.Exec(sql, args...)
	return
}

func (t *TidbClient) UpdateAppendObject(object *Object, tx DB) (err error) {
	if tx == nil {
		tx = t.Client
	}
	sql, args := object.GetAppendSql()
	_, err = tx.Exec(sql, args...)
	return err
}

func (t *TidbClient) PutObject(object *Object, tx DB) (err error) {
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
	return err
}

func (t *TidbClient) UpdateObject(object *Object, tx DB) (err error) {
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

	v := math.MaxUint64 - uint64(object.LastModifiedTime.UnixNano())
	version := strconv.FormatUint(v, 10)
	sqltext := "delete from objectpart where objectname=? and bucketname=? and version=?;"
	_, err = tx.Exec(sqltext, object.Name, object.BucketName, version)
	if err != nil {
		return err
	}

	sql, args := object.GetUpdateSql()
	_, err = tx.Exec(sql, args...)
	if object.Parts != nil {
		for _, p := range object.Parts {
			psql, args := p.GetCreateSql(object.BucketName, object.Name, version)
			_, err = tx.Exec(psql, args...)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *TidbClient) DeleteObject(object *Object, tx DB) (err error) {
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

	v := math.MaxUint64 - uint64(object.LastModifiedTime.UnixNano())
	version := strconv.FormatUint(v, 10)
	sqltext := "delete from objects where name=? and bucketname=? and version=?;"
	_, err = tx.Exec(sqltext, object.Name, object.BucketName, version)
	if err != nil {
		return err
	}
	sqltext = "delete from objectpart where objectname=? and bucketname=? and version=?;"
	_, err = tx.Exec(sqltext, object.Name, object.BucketName, version)
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
