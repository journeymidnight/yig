package tidbclient

import (
	"fmt"
	. "github.com/journeymidnight/yig/meta/types"
	"strconv"
)

//objmap
func (t *TidbClient) GetObjectMap(bucketName, objectName string) (objMap *ObjMap, err error) {
	objMap = &ObjMap{}
	sqltext := fmt.Sprintf("select bucketname,objectname,nullvernum from objmap where bucketname='%s' and objectName='%s'", bucketName, objectName)
	err = t.Client.QueryRow(sqltext).Scan(
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
	sqltext := fmt.Sprintf("insert into objmap values('%s','%s',%d)", objMap.BucketName, objMap.Name, objMap.NullVerNum)
	_, err := t.Client.Exec(sqltext)
	return err
}

func (t *TidbClient) DeleteObjectMap(objMap *ObjMap) error {
	sqltext := fmt.Sprintf("delete from objmap where bucketname='%s' and objectname='%s'", objMap.BucketName, objMap.Name)
	_, err := t.Client.Exec(sqltext)
	return err
}
