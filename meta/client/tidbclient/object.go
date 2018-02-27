package tidbclient

import (
	"database/sql"
	"encoding/json"
	"fmt"
	. "github.com/journeymidnight/yig/error"
	. "github.com/journeymidnight/yig/meta/types"
	"time"
)

func (t *TidbClient) GetObject(bucketName, objectName, version string) (object *Object, err error) {
	var ibucketname, iname, iversion, customattributes, acl, lastModifiedTime string
	var sqltext string
	if version == "" {
		sqltext = fmt.Sprintf("select * from objects where bucketname='%s' and name='%s'", bucketName, objectName)
	} else {
		sqltext = fmt.Sprintf("select * from objects where bucketname='%s' and name='%s' and version=%s", bucketName, objectName, version)
	}
	object = &Object{}
	err = t.Client.QueryRow(sqltext).Scan(
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
	)
	if err != nil && err == sql.ErrNoRows {
		err = ErrNoSuchKey
		return
	} else if err != nil {
		return
	}
	object.LastModifiedTime, err = time.Parse(TIME_LAYOUT_TIDB, lastModifiedTime)
	object.GetRowkey()
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
	if object.Etag == "" {
		object.Parts, err = getParts(object, t.Client)
	}
	return
}

func (t *TidbClient) PutObject(object *Object) error {
	var err error
	sql := object.GetCreateSql()
	_, err = t.Client.Exec(sql)
	if object.Parts != nil {
		for _, p := range object.Parts {
			psql := p.GetCreateSql()
			_, err = t.Client.Exec(psql)
			if err != nil {
				return err
			}
		}
	}
	return err
}

/*
func (t *TidbClient) DeleteObject(object *Object) error {
	sql, err := object.GetDeleteSql()
	if err != nil {
		return err
	}
	_, err = t.Client.Exec(sql)
	return err
}
*/
//util function
func getParts(object *Object, cli *sql.DB) (parts map[int]*Part, err error) {
	sql := fmt.Sprint("select partnumber,size,objectid,offset,etag,lastmodified,initializationvector from objectpart where key='%s'", object.Rowkey)
	rows, err := cli.Query(sql)
	defer rows.Close()
	if err != nil {
		return
	}
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
