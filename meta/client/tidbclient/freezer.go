package tidbclient

import (
	"database/sql"
	. "database/sql/driver"
	"math"
	"strconv"
	"time"

	. "github.com/journeymidnight/yig/error"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) CreateFreezer(freezer *Freezer) (err error) {
	sql, args := freezer.GetCreateSql()
	_, err = t.Client.Exec(sql, args...)
	return
}

func (t *TidbClient) GetFreezer(bucketName, objectName, version string) (freezer *Freezer, err error) {
	var lastmodifiedtime string
	var iversion uint64
	sqltext := "select bucketname,objectname,version,status,lifetime,lastmodifiedtime,IFNULL(location,''),IFNULL(pool,''),IFNULL(ownerid,''),IFNULL(size,'0'),IFNULL(objectid,''),IFNULL(etag,''),type,createtime " +
		"from restoreobjects where bucketname=? and objectname=? and version=?;"
	row := t.Client.QueryRow(sqltext, bucketName, objectName, version)
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
		&freezer.Type,
		&freezer.CreateTime,
	)
	if err == sql.ErrNoRows {
		err = ErrNoSuchKey
		return
	} else if err != nil {
		return
	}
	freezer.LastModifiedTime, err = time.Parse(TIME_LAYOUT_TIDB, lastmodifiedtime)
	if err != nil {
		return nil, err
	}
	if freezer.Type == ObjectTypeMultipart {
		iversion = math.MaxUint64 - freezer.CreateTime
		freezer.Parts, err = getFreezerParts(freezer.BucketName, freezer.Name, iversion, t.Client)
		//build simple index for multipart
		if len(freezer.Parts) != 0 {
			var sortedPartNum = make([]int64, len(freezer.Parts))
			for k, v := range freezer.Parts {
				sortedPartNum[k-1] = v.Offset
			}
			freezer.PartsIndex = &SimpleIndex{Index: sortedPartNum}
		}
	}
	return
}

func (t *TidbClient) GetFreezerStatus(bucketName, objectName, version string) (freezer *Freezer, err error) {
	sqltext := "select bucketname,objectname,version,status from restoreobjects where bucketname=? and objectname=? and version=?;"
	row := t.Client.QueryRow(sqltext, bucketName, objectName, version)
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

func (t *TidbClient) UpdateFreezerDate(bucketName, objectName, version string, lifetime int) (err error) {
	sqltext := "update restoreobjects set lifetime=? where bucketname=? and objectname=? and version=?;"
	_, err = t.Client.Exec(sqltext, lifetime, bucketName, objectName, version)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) DeleteFreezer(bucketName, objectName, versionId string, objectType ObjectType, createTime uint64, tx Tx) (err error) {
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
	sqltext := "delete from restoreobjects where bucketname=? and objectname=? and version=?;"
	_, err = txn.Exec(sqltext, bucketName, objectName, versionId)

	if err != nil {
		return err
	}
	if objectType == ObjectTypeMultipart {
		err = t.DeleteFreezerPart(bucketName, objectName, createTime, tx)
	}
	return err
}

func (t *TidbClient) DeleteFreezerPart(bucketName, objectName string, createTime uint64, tx Tx) (err error) {
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

	partVersion := strconv.FormatUint(math.MaxUint64-createTime, 10)
	sqltext := "delete from restoreobjectpart where bucketname=? and objectname=? and  version=?;"
	_, err = tx.(*sql.Tx).Exec(sqltext, bucketName, objectName, partVersion)
	if err != nil {
		return err
	}
	return nil
}

//common function
func getFreezerParts(bucketName, objectName string, iversion uint64, cli *sql.DB) (parts map[int]*Part, err error) {
	parts = make(map[int]*Part)
	sqltext := "select partnumber,size,objectid,offset,etag,lastmodified,initializationvector from restoreobjectpart where bucketname=? and objectname=? and version=?;"
	rows, err := cli.Query(sqltext, bucketName, objectName, iversion)
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
