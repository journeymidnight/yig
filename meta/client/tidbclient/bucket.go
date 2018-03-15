package tidbclient

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"strconv"
	"strings"
	"time"
)

func (t *TidbClient) GetBucket(bucketName string) (bucket Bucket, err error) {
	var acl, cors, lc, createTime string
	sqltext := fmt.Sprintf("select * from buckets where bucketname='%s';", bucketName)
	err = t.Client.QueryRow(sqltext).Scan(
		&bucket.Name,
		&acl,
		&cors,
		&lc,
		&bucket.OwnerId,
		&createTime,
		&bucket.Usage,
		&bucket.Versioning,
	)
	if err != nil && err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		return
	}
	bucket.CreateTime, err = time.Parse(TIME_LAYOUT_TIDB, createTime)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(acl), &bucket.ACL)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(cors), &bucket.CORS)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(lc), &bucket.LC)
	if err != nil {
		return
	}
	return
}

//Actually this method is used to update bucket
func (t *TidbClient) PutBucket(bucket Bucket) error {
	sql := bucket.GetUpdateSql()
	_, err := t.Client.Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) CheckAndPutBucket(bucket Bucket) (bool, error) {
	var processed bool
	b, err := t.GetBucket(bucket.Name)
	if err != nil {
		return processed, err
	}
	if b.Name != "" {
		return processed, nil
	} else {
		processed = true
	}
	sql := bucket.GetCreateSql()
	_, err = t.Client.Exec(sql)
	return processed, err
}

func (t *TidbClient) ListObjects(bucketName, marker, verIdMarker, prefix, delimiter string, versioned bool, maxKeys int) (retObjects []*Object, prefixes []string, truncated bool, nextMarker, nextVerIdMarker string, err error) {
	fmt.Println("enter list object")
	if versioned {
		return
	}
	var count int
	var exit bool
	objectMap := make(map[string]struct{})
	objectNum := make(map[string]int)
	commonPrefixes := make(map[string]struct{})
	omarker := marker
	for {
		fmt.Println("enter big loop")
		var loopcount int
		var sqltext string
		if marker == "" {
			sqltext = fmt.Sprintf("select bucketname,name,version,nullversion,deletemarker from objects where bucketName='%s' order by bucketname,name,version limit %d", bucketName, maxKeys)
		} else {
			sqltext = fmt.Sprintf("select bucketname,name,version,nullversion,deletemarker from objects where bucketName='%s' and name >='%s' order by bucketname,name,version limit %d,%d", bucketName, marker, objectNum[marker], objectNum[marker]+maxKeys)
		}
		fmt.Println("sqltext is:", sqltext)
		var rows *sql.Rows
		rows, err = t.Client.Query(sqltext)
		if err != nil && err == sql.ErrNoRows {
			exit = true
			break
		} else if err != nil {
			return
		}
		for rows.Next() {
			fmt.Println("enter small loop")
			loopcount += 1
			//fetch related date
			var bucketname, name string
			var version uint64
			var nullversion, deletemarker bool
			err = rows.Scan(
				&bucketname,
				&name,
				&version,
				&nullversion,
				&deletemarker,
			)
			if err != nil {
				fmt.Println(err)
				return
			}
			//prepare next marker
			//TODU: be sure how tidb/mysql compare strings
			if _, ok := objectNum[name]; !ok {
				objectNum[name] = 0
			}
			objectNum[name] += 1
			marker = name
			//filte row
			//filte by prefix
			hasPrefix := strings.HasPrefix(name, prefix)
			if !hasPrefix {
				continue
			}
			fmt.Println("has prefix")
			//filte by objectname
			if _, ok := objectMap[name]; !ok {
				objectMap[name] = struct{}{}
			} else {
				continue
			}
			fmt.Println("not same prefix")
			//filte by deletemarker
			if deletemarker {
				continue
			}
			fmt.Println("not deleted", delimiter, name)
			if name == omarker {
				continue
			}
			//filte by delemiter
			if len(delimiter) != 0 {
				subStr := strings.TrimPrefix(name, prefix)
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := prefix + string([]byte(subStr)[0:(n+1)])
					if prefixKey == omarker {
						continue
					}
					if _, ok := commonPrefixes[prefixKey]; !ok {
						if count == maxKeys {
							truncated = true
							exit = true
							break
						}
						commonPrefixes[prefixKey] = struct{}{}
						nextMarker = prefixKey
						count += 1
					}
					continue
				}
			}
			fmt.Println("not last", name)
			var o *Object
			Strver := strconv.FormatUint(version, 10)
			o, err = t.GetObject(bucketname, name, Strver)
			fmt.Println(o, err)
			if err != nil {
				return
			}
			count += 1
			if count == maxKeys {
				nextMarker = name
			}
			if count == 0 {
				continue
			}
			if count > maxKeys {
				truncated = true
				exit = true
				break
			}
			retObjects = append(retObjects, o)
		}
		if loopcount == 0 {
			exit = true
		}
		if exit {
			break
		}
	}
	prefixes = helper.Keys(commonPrefixes)
	return
}

func (t *TidbClient) DeleteBucket(bucket Bucket) error {
	sqltext := fmt.Sprintf("delete from buckets where bucketname='%s'", bucket.Name)
	_, err := t.Client.Exec(sqltext)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) UpdateUsage(bucketName string, size int64) {
	sql := fmt.Sprintf("update buckets set usages='%s' where bucketname='%s'", size, bucketName)
	t.Client.Exec(sql)
	return
}
