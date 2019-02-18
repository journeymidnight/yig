package tidbclient

import (
	"database/sql"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) PutBucketToLifeCycle(lifeCycle LifeCycle) error {
	sqltext := "insert into lifecycle(bucketname,status) values (?,?);"
	_, err := t.Client.Exec(sqltext, lifeCycle.BucketName, lifeCycle.Status)
	if err != nil {
		helper.Logger.Printf(0, "Failed in PutBucketToLifeCycle: %s\n", sqltext)
		return nil
	}
	return nil
}


func (t *TidbClient) RemoveBucketFromLifeCycle(bucket Bucket) error {
	sqltext := "delete from lifecycle where bucketname=?;"
	_, err := t.Client.Exec(sqltext, bucket.Name)
	if err != nil {
		helper.Logger.Printf(0, "Failed in RemoveBucketFromLifeCycle: %s\n", sqltext)
		return nil
	}
	return nil
}

func (t *TidbClient) ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error) {
	result.Truncated = false
	sqltext := "select * from lifecycle where bucketname > ? limit ?;"
	rows, err := t.Client.Query(sqltext, marker, limit)
	if err == sql.ErrNoRows {
		helper.Logger.Printf(0, "Failed in sql.ErrNoRows: %s\n", sqltext)
		err = nil
		return
	} else if err != nil {
		return
	}
	defer rows.Close()
	result.Lcs = make([]LifeCycle, 0, limit)
	var lc LifeCycle
	for rows.Next() {
		err = rows.Scan(
			&lc.BucketName,
			&lc.Status)
		if err != nil {
			helper.Logger.Printf(0, "Failed in ScanLifeCycle: %s ... %s\n", result.Lcs,result.NextMarker)
			return
		}
		result.Lcs = append(result.Lcs, lc)
	}
	result.NextMarker = lc.BucketName
	if len(result.Lcs) == limit{
		result.Truncated = true
	}
	return result,nil
}
