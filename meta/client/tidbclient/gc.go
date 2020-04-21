package tidbclient

import (
	"database/sql"
	. "database/sql/driver"
	"math"
	"time"

	. "github.com/journeymidnight/yig/meta/types"
)

//gc
func (t *TidbClient) PutObjectToGarbageCollection(object *Object, tx Tx) (err error) {
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

	o := GetGcInfoFromObject(object)
	var hasPart bool
	if len(o.Parts) > 0 {
		hasPart = true
	}
	mtime := o.MTime.Format(TIME_LAYOUT_TIDB)
	sqltext := "insert ignore into gc(bucketname,objectname,version,location,pool,objectid,status,mtime,part,triedtimes) values(?,?,?,?,?,?,?,?,?,?);"
	_, err = tx.(*sql.Tx).Exec(sqltext, o.BucketName, o.ObjectName, object.VersionId, o.Location, o.Pool, o.ObjectId, o.Status, mtime, hasPart, o.TriedTimes)
	if err != nil {
		return err
	}
	partVersion := math.MaxUint64 - object.CreateTime
	for _, p := range object.Parts {
		psql, args := p.GetCreateGcSql(o.BucketName, o.ObjectName, partVersion)
		_, err = tx.(*sql.Tx).Exec(psql, args...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *TidbClient) PutFreezerToGarbageCollection(f *Freezer, tx Tx) (err error) {
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
	object := f.ToObject()
	o := GetGcInfoFromObject(&object)
	var hasPart bool
	if len(o.Parts) > 0 {
		hasPart = true
	}
	mtime := o.MTime.Format(TIME_LAYOUT_TIDB)

	sqltext := "insert ignore into gc(bucketname,objectname,version,location,pool,objectid,status,mtime,part,triedtimes) values(?,?,?,?,?,?,?,?,?,?);"
	_, err = txn.Exec(sqltext, o.BucketName, o.ObjectName, f.VersionId, o.Location, o.Pool, o.ObjectId, o.Status, mtime, hasPart, o.TriedTimes)

	if err != nil {
		return err
	}

	partVersion := math.MaxUint64 - object.CreateTime
	for _, p := range object.Parts {
		psql, args := p.GetCreateGcSql(o.BucketName, o.ObjectName, partVersion)
		_, err = txn.Exec(psql, args...)
		if err != nil {
			return err
		}
	}
	return
}

func (t *TidbClient) ScanGarbageCollection(limit int) (gcs []GarbageCollection, err error) {
	var count int
	var sqltext string
	var rows *sql.Rows
	sqltext = "select bucketname,objectname,version from gc  order by bucketname,objectname,version limit ?;"
	rows, err = t.Client.Query(sqltext, limit)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var b, o, v string
		err = rows.Scan(
			&b,
			&o,
			&v,
		)
		var gc GarbageCollection = GarbageCollection{}
		gc, err = t.GetGarbageCollection(b, o, v)
		if err != nil {
			return
		}
		gcs = append(gcs, gc)
		count += 1
		if count >= limit {
			break
		}
	}
	return
}

func (t *TidbClient) RemoveGarbageCollection(garbage GarbageCollection) (err error) {
	var tx *sql.Tx
	tx, err = t.Client.Begin()
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

	sqltext := "delete from gc where bucketname=? and objectname=? and version=?;"
	_, err = tx.Exec(sqltext, garbage.BucketName, garbage.ObjectName, garbage.VersionId)
	if err != nil {
		return err
	}
	if len(garbage.Parts) > 0 {
		sqltext := "delete from gcpart where bucketname=? and objectname=? and version=?;"
		_, err := tx.Exec(sqltext, garbage.BucketName, garbage.ObjectName, garbage.VersionId)
		if err != nil {
			return err
		}
	}
	return nil
}

//common func
func (t *TidbClient) GetGarbageCollection(bucketName, objectName, version string) (gc GarbageCollection, err error) {
	sqltext := "select bucketname,objectname,version,location,pool,objectid,status,mtime,part,triedtimes from gc where bucketname=? and objectname=? and version=?;"
	var hasPart bool
	var mtime string
	var v string
	err = t.Client.QueryRow(sqltext, bucketName, objectName, version).Scan(
		&gc.BucketName,
		&gc.ObjectName,
		&v,
		&gc.Location,
		&gc.Pool,
		&gc.ObjectId,
		&gc.Status,
		&mtime,
		&hasPart,
		&gc.TriedTimes,
	)
	gc.MTime, err = time.Parse(TIME_LAYOUT_TIDB, mtime)
	if err != nil {
		return
	}
	if hasPart {
		var p map[int]*Part
		p, err = getGcParts(bucketName, objectName, version, t.Client)
		if err != nil {
			return
		}
		gc.Parts = p
	}
	return
}

func getGcParts(bucketname, objectname, version string, cli *sql.DB) (parts map[int]*Part, err error) {
	parts = make(map[int]*Part)
	sqltext := "select partnumber,size,objectid,offset,etag,lastmodified,initializationvector from gcpart where bucketname=? and objectname=? and version=?;"
	rows, err := cli.Query(sqltext, bucketname, objectname, version)
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
