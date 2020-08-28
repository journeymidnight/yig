package main

import (
	"database/sql"
	"encoding/json"
	"math"
	"time"

	"github.com/journeymidnight/yig/meta/client/tidbclient"
	"github.com/journeymidnight/yig/meta/types"
)

func GetObjectsByBucket(t *tidbclient.TidbClient, bucketName string) ([]*types.Object, error) {
	var objects []*types.Object
	var customattributes, acl, lastModifiedTime string
	sqltext := "select bucketname,name,version,location,pool,ownerid,size,objectid,lastmodifiedtime,etag,contenttype," +
		"customattributes,acl,nullversion,deletemarker,ssetype,encryptionkey,initializationvector,type,storageclass,createtime from objects where bucketname=?;"
	rows, err := t.Client.Query(sqltext, bucketName)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		o := new(types.Object)
		err = rows.Scan(
			&o.BucketName,
			&o.Name,
			&o.VersionId,
			&o.Location,
			&o.Pool,
			&o.OwnerId,
			&o.Size,
			&o.ObjectId,
			&lastModifiedTime,
			&o.Etag,
			&o.ContentType,
			&customattributes,
			&acl,
			&o.NullVersion,
			&o.DeleteMarker,
			&o.SseType,
			&o.EncryptionKey,
			&o.InitializationVector,
			&o.Type,
			&o.StorageClass,
			&o.CreateTime,
		)
		o.LastModifiedTime, err = time.Parse(types.TIME_LAYOUT_TIDB, lastModifiedTime)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal([]byte(acl), &o.ACL)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal([]byte(customattributes), &o.CustomAttributes)
		if err != nil {
			return nil, err
		}
		if o.Type == types.ObjectTypeMultipart {
			partVersion := math.MaxUint64 - o.CreateTime
			o.Parts, err = getParts(o.BucketName, o.Name, partVersion, t.Client)
			if err != nil {
				return nil, err
			}
			//build simple index for multipart
			if len(o.Parts) != 0 {
				var sortedPartNum = make([]int64, len(o.Parts))
				for k, v := range o.Parts {
					sortedPartNum[k-1] = v.Offset
				}
				o.PartsIndex = &types.SimpleIndex{Index: sortedPartNum}
			}
		}
		objects = append(objects, o)
	}
	return objects, nil
}

func getParts(bucketName, objectName string, version uint64, cli *sql.DB) (parts map[int]*types.Part, err error) {
	parts = make(map[int]*types.Part)
	sqltext := "select partnumber,size,objectid,offset,etag,lastmodified,initializationvector from objectpart where bucketname=? and objectname=? and version=?;"
	rows, err := cli.Query(sqltext, bucketName, objectName, version)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var p = &types.Part{}
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
