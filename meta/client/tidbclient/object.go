package tidbclient

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	. "github.com/journeymidnight/yig/error"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/xxtea/xxtea-go/xxtea"
	"math"
	"strconv"
	"time"
)

func (t *TidbClient) GetObject(bucketName, objectName, version string) (object *Object, err error) {
	fmt.Println("enter meta getobject")
	var ibucketname, iname, customattributes, acl, lastModifiedTime string
	var iversion uint64
	var sqltext string
	if version == "" {
		sqltext = fmt.Sprintf("select * from objects where bucketname='%s' and name='%s' order by bucketname,name,version limit 1", bucketName, objectName)
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
	rversion := math.MaxUint64 - iversion
	s := int64(rversion) / 1e9
	ns := int64(rversion) % 1e9
	object.LastModifiedTime = time.Unix(s, ns)
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
	object.Parts, err = getParts(object.BucketName, object.Name, iversion, t.Client)
	//build simple index for multipart
	fmt.Println("len is:", len(object.Parts))
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
	sqltext := fmt.Sprintf("select version from objects where bucketname='%s' and name='%s'", bucketName, objectName)
	var versions []string
	rows, err := t.Client.Query(sqltext)
	if err != nil {
		return
	}
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

func (t *TidbClient) PutObject(object *Object) error {
	var err error
	sql := object.GetCreateSql()
	_, err = t.Client.Exec(sql)
	if object.Parts != nil {
		v := math.MaxUint64 - uint64(object.LastModifiedTime.UnixNano())
		version := strconv.FormatUint(v, 10)
		for _, p := range object.Parts {
			psql := p.GetCreateSql(object.BucketName, object.Name, version)
			_, err = t.Client.Exec(psql)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func (t *TidbClient) DeleteObject(object *Object) error {
	fmt.Println("enter meta deleteobject")
	v := math.MaxUint64 - uint64(object.LastModifiedTime.UnixNano())
	version := strconv.FormatUint(v, 10)
	sqltext := fmt.Sprintf("delete from objects where name='%s' and bucketname='%s' and version='%s'", object.Name, object.BucketName, version)
	_, err := t.Client.Exec(sqltext)
	fmt.Println(sqltext)
	if err != nil {
		return err
	}
	sqltext = fmt.Sprintf("delete from objectpart where objectname='%s' and bucketname='%s' and version='%s'", object.Name, object.BucketName, version)
	_, err = t.Client.Exec(sqltext)
	fmt.Println(sqltext)
	if err != nil {
		return err
	}
	return nil
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
func getParts(bucketName, objectName string, version uint64, cli *sql.DB) (parts map[int]*Part, err error) {
	parts = make(map[int]*Part)
	sqltext := fmt.Sprintf("select partnumber,size,objectid,offset,etag,lastmodified,initializationvector from objectpart where bucketname='%s' and objectname='%s' and version=%d", bucketName, objectName, version)
	fmt.Println(sqltext)
	rows, err := cli.Query(sqltext)
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
