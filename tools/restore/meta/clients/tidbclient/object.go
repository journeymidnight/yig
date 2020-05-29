package tidbclient

import (
	"database/sql"
	"encoding/json"
	"math"
	"time"

	. "github.com/journeymidnight/yig-restore/error"
	. "github.com/journeymidnight/yig-restore/meta/types"
)

func (t *TidbClient) GetObject(bucketName, objectName, version string) (*Object, error) {
	var ibucketname, iname, customattributes, acl, lastModifiedTime string
	var iversion uint64
	var err error
	var row *sql.Row
	sqltext := "select bucketname,name,version,location,pool,ownerid,size,objectid,lastmodifiedtime,etag,contenttype," +
		"customattributes,acl,nullversion,deletemarker,ssetype,encryptionkey,initializationvector,type,storageclass,createtime from objects where bucketname=? and name=? and version=?;"
	row = t.Client.QueryRow(sqltext, bucketName, objectName, version)
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
	object.LastModifiedTime, err = time.Parse(TIME_LAYOUT_TIDB, lastModifiedTime)
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

//common function
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
