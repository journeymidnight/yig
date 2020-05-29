package tidbclient

import (
	"database/sql"
	. "database/sql/driver"
	"math"

	. "github.com/journeymidnight/yig-restore/meta/types"
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
	version := math.MaxUint64 - uint64(object.LastModifiedTime.UnixNano())
	sqltext := "insert ignore into gc(bucketname,objectname,version,location,pool,objectid,status,mtime,part,triedtimes) values(?,?,?,?,?,?,?,?,?,?);"
	_, err = tx.(*sql.Tx).Exec(sqltext, o.BucketName, o.ObjectName, object.VersionId, o.Location, o.Pool, o.ObjectId, o.Status, mtime, hasPart, o.TriedTimes)
	if err != nil {
		return err
	}
	for _, p := range object.Parts {
		psql, args := p.GetCreateGcSql(o.BucketName, o.ObjectName, version)
		_, err = tx.(*sql.Tx).Exec(psql, args...)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *TidbClient) PutFreezerToGarbageCollection(object *Freezer, tx Tx) (err error) {
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
	o := GarbageCollectionFromFreeze(object)
	var hasPart bool
	if len(o.Parts) > 0 {
		hasPart = true
	}
	mtime := o.MTime.Format(TIME_LAYOUT_TIDB)
	version := math.MaxUint64 - uint64(object.LastModifiedTime.UnixNano())
	sqltext := "insert ignore into gc(bucketname,objectname,version,location,pool,objectid,status,mtime,part,triedtimes) values(?,?,?,?,?,?,?,?,?,?);"
	_, err = txn.Exec(sqltext, o.BucketName, o.ObjectName, version, o.Location, o.Pool, o.ObjectId, o.Status, mtime, hasPart, o.TriedTimes)
	if err != nil {
		return err
	}
	for _, p := range object.Parts {
		psql, args := p.GetCreateGcSql(o.BucketName, o.ObjectName, version)
		_, err = txn.Exec(psql, args...)
		if err != nil {
			return err
		}
	}
	return
}
