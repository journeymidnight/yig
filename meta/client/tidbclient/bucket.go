package tidbclient

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
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
	bucket.CreateTime, err = time.Parse(TIME_LAYOUT_TIDB, createTime)
	if err != nil {
		return
	}
	if err != nil && err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
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
	if versioned {
		return
	}
	var count int
	var exit bool
	objectMap := make(map[string]struct{})
	commonPrefixes := make(map[string]struct{})
	for {
		var loopcount int
		sqltext := fmt.Sprintf("select bucketname,name,version,nullversion,deletemarker from objects where bucketName='%s' and name >='%s' order by bucketname,name,version limit %d", bucketName, marker, maxKeys)
		var rows *sql.Rows
		rows, err = t.Client.Query(sqltext)
		if err != nil && err == sql.ErrNoRows {
			exit = true
			break
		} else if err != nil {
			return
		}
		for rows.Next() {
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
				return
			}
			//prepare next marker
			//TODU: be sure how tidb/mysql compare strings
			marker = name + ObjectNameSmallestStr
			//filte row
			//filte by prefix
			hasPrefix := strings.HasPrefix(name, prefix)
			if !hasPrefix {
				continue
			}
			//filte by objectname
			if _, ok := objectMap[name]; !ok {
				objectMap[name] = struct{}{}
			} else {
				continue
			}
			//filte by deletemarker
			if deletemarker {
				continue
			}
			//filte by delemiter
			if len(delimiter) != 0 {
				subStr := strings.TrimPrefix(name, prefix)
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := string([]byte(subStr)[0:(n + 1)])
					if _, ok := commonPrefixes[prefixKey]; !ok {
						commonPrefixes[prefixKey] = struct{}{}
						if count >= maxKeys {
							truncated = true
							exit = true
							nextMarker = name
							break
						}
						count += 1
					}
					continue
				}
			}
			if count >= maxKeys {
				truncated = true
				exit = true
				nextMarker = name
				break
			}
			var o *Object
			Strver := strconv.FormatUint(version, 10)
			o, err = t.GetObject(bucketname, name, Strver)
			if err != nil {
				return
			}
			retObjects = append(retObjects, o)
			count += 1
		}
		if loopcount < maxKeys {
			exit = true
		}
		if exit {
			break
		}
	}
	return
}

func (t *TidbClient) UpdateUsage(bucketName string, size int64) {
	sql := fmt.Sprintf("update buckets set usages='%s' where bucketname='%s'", size, bucketName)
	t.Client.Exec(sql)
	return
}
