package tidbclient

import (
	"database/sql"

	. "github.com/journeymidnight/yig/error"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) PutBucketToLifeCycle(bucket Bucket, lifeCycle LifeCycle) error {
	tx, err := t.Client.Begin()
	if err != nil {
		return NewError(InTidbFatalError, "PutBucketToLifeCycle transaction starts err", err)
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		}
		if err != nil {
			tx.Rollback()
		}
	}()

	bucketSql, bucketArgs := bucket.GetUpdateSql()
	_, err = tx.Exec(bucketSql, bucketArgs...)

	sqltext, args := lifeCycle.GetCreateSql()
	_, err = tx.Exec(sqltext, args...)
	if err != nil {
		return NewError(InTidbFatalError, "PutBucketToLifeCycle transaction executes err", err)
	}
	return nil
}

func (t *TidbClient) GetBucketLifeCycle(bucket Bucket) (*LifeCycle, error) {
	lc := LifeCycle{}
	sqltext := "select bucketname,status,starttime,endtime from lifecycle where bucketname=?;"
	err := t.Client.QueryRow(sqltext, bucket.Name).Scan(
		&lc.BucketName,
		&lc.Status,
		&lc.StartTime,
		&lc.EndTime,
	)
	if err != nil && err != sql.ErrNoRows {
		return nil, NewError(InTidbFatalError, "GetBucketLifeCycle scan row err", err)
	}
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &lc, nil
}

func (t *TidbClient) RemoveBucketFromLifeCycle(bucket Bucket) error {
	tx, err := t.Client.Begin()
	if err != nil {
		NewError(InTidbFatalError, "RemoveBucketFromLifeCycle transaction starts err", err)
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		}
		if err != nil {
			tx.Rollback()
		}
	}()

	bucketSql, bucketArgs := bucket.GetUpdateSql()
	_, err = tx.Exec(bucketSql, bucketArgs...)

	sqltext := "delete from lifecycle where bucketname=?;"
	_, err = tx.Exec(sqltext, bucket.Name)

	if err != nil {
		return NewError(InTidbFatalError, "RemoveBucketFromLifeCycle transaction executes err", err)
	}
	return nil
}

func (t *TidbClient) ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error) {
	result.Truncated = false
	sqltext := "select bucketname,status,starttime,endtime from lifecycle where bucketname > ? limit ?;"
	rows, err := t.Client.Query(sqltext, marker, limit)
	if err == sql.ErrNoRows {
		return result, nil
	} else if err != nil {
		return result, NewError(InTidbFatalError, "ScanLifeCycle query err", err)
	}
	defer rows.Close()
	result.Lcs = make([]LifeCycle, 0, limit)
	var lc LifeCycle
	for rows.Next() {
		err = rows.Scan(
			&lc.BucketName,
			&lc.Status,
			&lc.StartTime,
			&lc.EndTime)
		if err != nil {
			return result, NewError(InTidbFatalError, "ScanLifeCycle scan row err", err)
		}
		result.Lcs = append(result.Lcs, lc)
	}
	result.NextMarker = lc.BucketName
	if len(result.Lcs) == limit {
		result.Truncated = true
	}
	return result, nil
}
