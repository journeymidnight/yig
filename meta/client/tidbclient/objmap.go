package tidbclient

import (
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

func (t *TidbClient) PutObjectMap(objMap *ObjMap) error {
	sqltext := "insert into objmap(bucketname,objectname,nullvernum) values(?,?,?);"
	_, err := t.Client.Exec(sqltext, objMap.BucketName, objMap.Name, objMap.NullVerNum)
	return err
}

func (t *TidbClient) DeleteObjectMap(objMap *ObjMap) error {
	sqltext := "delete from objmap where bucketname=? and objectname=?;"
	_, err := t.Client.Exec(sqltext, objMap.BucketName, objMap.Name)
	return err
}
