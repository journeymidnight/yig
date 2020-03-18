package tidbclient

import (
	"database/sql"
	. "github.com/journeymidnight/yig/meta/types"
	"math"
	"strings"
	"time"
)

//gc
func (t *TidbClient) PutObjectToGarbageCollection(object *Object, tx DB) (err error) {
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

	o := GarbageCollectionFromObject(object)
	var hasPart bool
	if len(o.Parts) > 0 {
		hasPart = true
	}
	mtime := o.MTime.Format(TIME_LAYOUT_TIDB)
	version := math.MaxUint64 - uint64(object.LastModifiedTime.UnixNano())
	sqltext := "insert ignore into gc(bucketname,objectname,version,location,pool,objectid,status,mtime,part,triedtimes) values(?,?,?,?,?,?,?,?,?,?);"
	_, err = tx.Exec(sqltext, o.BucketName, o.ObjectName, version, o.Location, o.Pool, o.ObjectId, o.Status, mtime, hasPart, o.TriedTimes)
	if err != nil {
		return err
	}
	for _, p := range object.Parts {
		psql, args := p.GetCreateGcSql(o.BucketName, o.ObjectName, version)
		_, err = tx.Exec(psql, args...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *TidbClient) ScanGarbageCollection(limit int, startRowKey string) (gcs []GarbageCollection, err error) {
	var count int
	var sqltext string
	var rows *sql.Rows
	if startRowKey == "" {
		sqltext = "select bucketname,objectname,version from gc  order by bucketname,objectname,version limit ?;"
		rows, err = t.Client.Query(sqltext, limit)
	} else {
		s := strings.Split(startRowKey, ObjectNameSeparator)
		bucketname := s[0]
		objectname := s[1]
		version := s[2]
		sqltext = "select bucketname,objectname,version from gc where bucketname>? or (bucketname=? and objectname>?) or (bucketname=? and objectname=? and version >= ?) limit ?;"
		rows, err = t.Client.Query(sqltext, bucketname, bucketname, objectname, bucketname, objectname, version, limit)
	}
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

	version := strings.Split(garbage.Rowkey, ObjectNameSeparator)[2]
	sqltext := "delete from gc where bucketname=? and objectname=? and version=?;"
	_, err = tx.Exec(sqltext, garbage.BucketName, garbage.ObjectName, version)
	if err != nil {
		return err
	}
	if len(garbage.Parts) > 0 {
		sqltext := "delete from gcpart where bucketname=? and objectname=? and version=?;"
		_, err := tx.Exec(sqltext, garbage.BucketName, garbage.ObjectName, version)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *TidbClient) PutFreezerToGarbageCollection(object *Freezer, tx DB) (err error) {
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
	o := GarbageCollectionFromFreeze(object)
	var hasPart bool
	if len(o.Parts) > 0 {
		hasPart = true
	}
	mtime := o.MTime.Format(TIME_LAYOUT_TIDB)
	version := math.MaxUint64 - uint64(object.LastModifiedTime.UnixNano())
	sqltext := "insert ignore into gc(bucketname,objectname,version,location,pool,objectid,status,mtime,part,triedtimes) values(?,?,?,?,?,?,?,?,?,?);"
	_, err = tx.Exec(sqltext, o.BucketName, o.ObjectName, version, o.Location, o.Pool, o.ObjectId, o.Status, mtime, hasPart, o.TriedTimes)
	if err != nil {
		return err
	}
	for _, p := range object.Parts {
		psql, args := p.GetCreateGcSql(o.BucketName, o.ObjectName, version)
		_, err = tx.Exec(psql, args...)
		if err != nil {
			return err
		}
	}
	return
}

//util func
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
	gc.Rowkey = gc.BucketName + ObjectNameSeparator + gc.ObjectName + ObjectNameSeparator + v
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

func GarbageCollectionFromObject(o *Object) (gc GarbageCollection) {
	gc.BucketName = o.BucketName
	gc.ObjectName = o.Name
	gc.Location = o.Location
	gc.Pool = o.Pool
	gc.ObjectId = o.ObjectId
	gc.Status = "Pending"
	gc.MTime = time.Now().UTC()
	gc.Parts = o.Parts
	gc.TriedTimes = 0
	return
}

func GarbageCollectionFromFreeze(f *Freezer) (gc GarbageCollection) {
	gc.BucketName = f.BucketName
	gc.ObjectName = f.Name
	gc.Location = f.Location
	gc.Pool = f.Pool
	gc.ObjectId = f.ObjectId
	gc.Status = "Pending"
	gc.MTime = time.Now().UTC()
	gc.Parts = f.Parts
	gc.TriedTimes = 0
	return
}
