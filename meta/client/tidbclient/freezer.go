package tidbclient

import (
	"database/sql"
	. "github.com/journeymidnight/yig/error"
	. "github.com/journeymidnight/yig/meta/types"
	"strconv"
)

func (t *TidbClient) CreateFreezer(freezer *Freezer) (err error) {
	sql, args := freezer.GetCreateSql()
	_, err = t.Client.Exec(sql, args...)
	return
}

func (t *TidbClient) ListFreezers(maxKeys int) (retFreezers []Freezer, err error) {
	var count int
	var marker string
	marker = ""
	for {
		if marker == "" {
			count = 0
		}
		var loopCount int
		loopCount = 0
		var sqltext string
		var rows *sql.Rows
		if marker == "" {
			sqltext = "select bucketname,objectname,version,status,lifetime,lastmodifiedtime from restoreobjects order by bucketname,objectname,version limit ?;"
			rows, err = t.Client.Query(sqltext, maxKeys)
		} else {
			sqltext = "select bucketname,objectname,version,status,lifetime,lastmodifiedtime from restoreobjects where name >=? order by bucketname,objectname,version limit ?,?;"
			rows, err = t.Client.Query(sqltext, marker, count-1, count+maxKeys)
		}
		if err != nil {
			return
		}
		defer rows.Close()
		for rows.Next() {
			count += 1
			loopCount += 1
			var version uint64
			retFreezer := &Freezer{}
			err = rows.Scan(
				&retFreezer.BucketName,
				&retFreezer.Name,
				&version,
				&retFreezer.LifeTime,
				&retFreezer.LastModifiedTime,
			)
			if err != nil {
				return
			}
			str := strconv.FormatUint(version, 10)
			retFreezer.VersionId = str
			retFreezers = append(retFreezers, *retFreezer)
			marker = retFreezer.Name
		}
		if loopCount < maxKeys {
			break
		}
	}
	return
}

func (t *TidbClient) ListFreezersNeedContinue(maxKeys int, status Status) (retFreezers []Freezer, err error) {
	var count int
	var marker string
	marker = ""
	for {
		if marker == "" {
			count = 0
		}
		var loopCount int
		loopCount = 0
		var sqltext string
		var rows *sql.Rows
		if marker == "" {
			sqltext = "select bucketname,objectname,version from restoreobjects where status=? order by bucketname,objectname,version limit ?;"
			rows, err = t.Client.Query(sqltext, status, maxKeys)
		} else {
			sqltext = "select bucketname,objectname,version from restoreobjects where name >=? and status=? order by bucketname,objectname,version limit ?,?;"
			rows, err = t.Client.Query(sqltext, marker, status, count-1, count+maxKeys)
		}
		if err != nil {
			return
		}
		defer rows.Close()
		for rows.Next() {
			count += 1
			loopCount += 1
			var version uint64
			retFreezer := &Freezer{}
			err = rows.Scan(
				&retFreezer.BucketName,
				&retFreezer.Name,
				&version,
				&retFreezer.LifeTime,
				&retFreezer.LastModifiedTime,
			)
			if err != nil {
				return
			}
			str := strconv.FormatUint(version, 10)
			retFreezer.VersionId = str
			retFreezers = append(retFreezers, *retFreezer)
			marker = retFreezer.Name
		}
		if loopCount < maxKeys {
			break
		}
	}
	return
}

func (t *TidbClient) GetFreezer(bucketName, objectName, version string) (freezer *Freezer, err error) {
	sqltext := "select bucketname,objectname,COALESCE(version,''),status,lifetime,lastmodifiedtime,location,pool,ownerid,size,etag,initializationvector from restoreobjects where bucketname=? and objectname=?;"
	row := t.Client.QueryRow(sqltext, bucketName, objectName)
	freezer = &Freezer{}
	err = row.Scan(
		&freezer.BucketName,
		&freezer.Name,
		&freezer.VersionId,
		&freezer.Status,
		&freezer.LifeTime,
		&freezer.LastModifiedTime,
		&freezer.Location,
		&freezer.Pool,
		&freezer.OwnerId,
		&freezer.Size,
		&freezer.Etag,
		&freezer.InitializationVector,
	)
	if err == sql.ErrNoRows {
		err = ErrNoSuchKey
		return
	} else if err != nil {
		return
	}
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
	sqltext := "select bucketname,objectname,version,status from restoreobjects where bucketname=? and objectname=? and version=?;"
	row := t.Client.QueryRow(sqltext, bucketName, objectName, version)
	freezer = &Freezer{}
	err = row.Scan(
		&freezer.BucketName,
		&freezer.Name,
		&freezer.VersionId,
		&freezer.Status,
	)
	if err == sql.ErrNoRows || freezer.Name == "" {
		err = ErrNoSuchKey
		return
	}
	return
}

func (t *TidbClient) UploadFreezerStatus(bucketName, objectName string, status, statusSetting Status) (err error) {
	sqltext := "update restoreobjects set status=? where bucketname=? and objectname=? and status=?;"
	_, err = t.Client.Exec(sqltext, statusSetting, bucketName, objectName, status)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) UploadFreezerBackendInfo(targetFreezer *Freezer) (err error) {
	sqltext := "update restoreobjects set pool=?,size=? where bucketname=? and objectname=?;"
	_, err = t.Client.Exec(sqltext, targetFreezer.Pool, targetFreezer.Size, targetFreezer.BucketName, targetFreezer.Name)
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
