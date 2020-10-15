package tidbclient

import (
	"database/sql"

	. "github.com/journeymidnight/yig/error"
)

func (t *TidbClient) GetUserBuckets(userId string) (buckets []string, err error) {
	sqltext := "select bucketname from users where userid=?;"
	rows, err := t.Client.Query(sqltext, userId)
	if err == sql.ErrNoRows {
		return buckets, nil
	} else if err != nil {
		return buckets, NewError(InTidbFatalError, "GetUserBuckets query err", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tmp string
		err = rows.Scan(&tmp)
		if err != nil {
			return buckets, NewError(InTidbFatalError, "GetUserBuckets scan row err", err)
		}
		buckets = append(buckets, tmp)
	}
	return
}

func (t *TidbClient) RemoveBucketForUser(bucketName string, userId string) (err error) {
	sql := "delete from users where userid=? and bucketname=?;"
	_, err = t.Client.Exec(sql, userId, bucketName)
	if err != nil {
		return NewError(InTidbFatalError, "RemoveBucketForUser transactions executes err", err)
	}
	return
}

func (t *TidbClient) GetAllUserBuckets() (bucketUser map[string]string, err error) {
	// bucket name -> user id
	bucketUser = make(map[string]string)
	rows, err := t.Client.Query("select userid, bucketname from users")
	if err != nil {
		return bucketUser, NewError(InTidbFatalError, "GetAllUserBuckets query err", err)
	}
	defer rows.Close()

	for rows.Next() {
		var userID, bucketName string
		err = rows.Scan(&userID, &bucketName)
		if err != nil {
			return bucketUser, NewError(InTidbFatalError, "GetAllUserBuckets scan row err", err)
		}
		bucketUser[bucketName] = userID
	}
	return
}
