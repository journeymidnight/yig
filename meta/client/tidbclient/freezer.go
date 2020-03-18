package tidbclient

import (
	"database/sql"
	. "github.com/journeymidnight/yig/error"
	. "github.com/journeymidnight/yig/meta/types"
	"time"
)

func (t *TidbClient) CreateFreezer(freezer *Freezer) (err error) {
	sql, args := freezer.GetCreateSql()
	_, err = t.Client.Exec(sql, args...)
	return
}

func (t *TidbClient) GetFreezer(bucketName, objectName, version string) (freezer *Freezer, err error) {
	var lastmodifiedtime string
	sqltext := "select bucketname,objectname,IFNULL(version,''),status,lifetime,lastmodifiedtime,IFNULL(location,''),IFNULL(pool,''),IFNULL(ownerid,''),IFNULL(size,'0'),IFNULL(objectid,''),IFNULL(etag,'') from restoreobjects where bucketname=? and objectname=?;"
	row := t.Client.QueryRow(sqltext, bucketName, objectName)
	freezer = &Freezer{}
	err = row.Scan(
		&freezer.BucketName,
		&freezer.Name,
		&freezer.VersionId,
		&freezer.Status,
		&freezer.LifeTime,
		&lastmodifiedtime,
		&freezer.Location,
		&freezer.Pool,
		&freezer.OwnerId,
		&freezer.Size,
		&freezer.ObjectId,
		&freezer.Etag,
	)
	if err == sql.ErrNoRows {
		err = ErrNoSuchKey
		return
	} else if err != nil {
		return
	}
	local, _ := time.LoadLocation("Local")
	freezer.LastModifiedTime, _ = time.ParseInLocation(TIME_LAYOUT_TIDB, lastmodifiedtime, local)
	freezer.Parts, err = getFreezerParts(freezer.BucketName, freezer.Name, t.Client)
	//build simple index for multipart
	if len(freezer.Parts) != 0 {
		var sortedPartNum = make([]int64, len(freezer.Parts))
		for k, v := range freezer.Parts {
			sortedPartNum[k-1] = v.Offset
		}
		freezer.PartsIndex = &SimpleIndex{Index: sortedPartNum}
	}
	return
}

func (t *TidbClient) GetFreezerStatus(bucketName, objectName, version string) (freezer *Freezer, err error) {
	sqltext := "select bucketname,objectname,IFNULL(version,''),status from restoreobjects where bucketname=? and objectname=?;"
	row := t.Client.QueryRow(sqltext, bucketName, objectName)
	freezer = &Freezer{}
	err = row.Scan(
		&freezer.BucketName,
		&freezer.Name,
		&freezer.VersionId,
		&freezer.Status,
	)
	if err == sql.ErrNoRows || freezer.Name != objectName {
		err = ErrNoSuchKey
		return
	}
	return
}

func (t *TidbClient) UploadFreezerDate(bucketName, objectName string, lifetime int) (err error) {
	sqltext := "update restoreobjects set lifetime=? where bucketname=? and objectname=?;"
	_, err = t.Client.Exec(sqltext, lifetime, bucketName, objectName)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) DeleteFreezer(bucketName, objectName string, tx DB) (err error) {
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
	sqltext := "delete from restoreobjects where bucketname=? and objectname=?;"
	_, err = tx.Exec(sqltext, bucketName, objectName)
	if err != nil {
		return err
	}
	sqltext = "delete from restoreobjectpart where objectname=? and bucketname=?;"
	_, err = tx.Exec(sqltext, bucketName, objectName)
	if err != nil {
		return err
	}
	return nil
}

//util function
func getFreezerParts(bucketName, objectName string, cli *sql.DB) (parts map[int]*Part, err error) {
	parts = make(map[int]*Part)
	sqltext := "select partnumber,size,objectid,offset,etag,lastmodified,initializationvector from restoreobjectpart where bucketname=? and objectname=?;"
	rows, err := cli.Query(sqltext, bucketName, objectName)
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
