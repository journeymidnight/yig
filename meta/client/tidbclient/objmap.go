package tidbclient

import (
	"database/sql"
	. "github.com/journeymidnight/yig/meta/types"
	"strconv"
)

//objmap
func (t *TidbClient) GetObjectMap(bucketName, objectName string) (objMap *ObjMap, err error) {
	objMap = &ObjMap{}
	sqltext := "select bucketname,objectname,nullvernum from objmap where bucketname=? and objectName=?;"
	err = t.Client.QueryRow(sqltext, bucketName, objectName).Scan(
		&objMap.BucketName,
		&objMap.Name,
		&objMap.NullVerNum,
	)
	if err != nil {
		return
	}
	objMap.NullVerId = strconv.FormatUint(objMap.NullVerNum, 10)
	return
}

func (t *TidbClient) PutObjectMap(objMap *ObjMap, tx interface{}) (err error) {
	var sqlTx *sql.Tx
	if tx == nil {
		tx, err = t.Client.Begin()
		defer func() {
			if err == nil {
				err = sqlTx.Commit()
			}
			if err != nil {
				sqlTx.Rollback()
			}
		}()
	}
	sqlTx, _ = tx.(*sql.Tx)

	sqltext := "insert into objmap(bucketname,objectname,nullvernum) values(?,?,?);"
	_, err = sqlTx.Exec(sqltext, objMap.BucketName, objMap.Name, objMap.NullVerNum)
	return err
}

func (t *TidbClient) DeleteObjectMap(objMap *ObjMap, tx interface{}) (err error) {
	var sqlTx *sql.Tx
	if tx == nil {
		tx, err = t.Client.Begin()
		defer func() {
			if err == nil {
				err = sqlTx.Commit()
			}
			if err != nil {
				sqlTx.Rollback()
			}
		}()
	}
	sqlTx, _ = tx.(*sql.Tx)
	sqltext := "delete from objmap where bucketname=? and objectname=?;"
	_, err = sqlTx.Exec(sqltext, objMap.BucketName, objMap.Name)
	return err
}
