package tidbclient

import (
	"database/sql"
	"fmt"
)

func (t *TidbClient) GetUserBuckets(userId string) (buckets []string, err error) {
	sqltext := fmt.Sprintf("select bucketname from users where userid='%s'", userId)
	rows, err := t.Client.Query(sqltext)
	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var tmp string
		err = rows.Scan(&tmp)
		if err != nil {
			return
		}
		buckets = append(buckets, tmp)
	}
	return
}

func (t *TidbClient) AddBucketForUser(bucketName, userId string) (err error) {
	sql := fmt.Sprintf("insert into users values('%s','%s')", userId, bucketName)
	_, err = t.Client.Exec(sql)
	return
}

func (t *TidbClient) RemoveBucketForUser(bucketName string, userId string) (err error) {
	sql := fmt.Sprintf("delete from users where userid='%s' and bucketname='%s'", userId, bucketName)
	_, err = t.Client.Exec(sql)
	return
}
