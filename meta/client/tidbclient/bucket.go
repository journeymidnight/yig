package tidbclient

import (
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) GetBucket(bucketName string) (bucket *Bucket, err error) {
	var acl, cors, logging, lc, policy, website, encryption, createTime string
	sqltext := "select bucketname,acl,cors,COALESCE(logging,\"\"),lc,uid,policy,website,COALESCE(encryption,\"\"),createtime,usages,versioning from buckets where bucketname=?;"
	bucket = new(Bucket)
	err = t.Client.QueryRow(sqltext, bucketName).Scan(
		&bucket.Name,
		&acl,
		&cors,
		&logging,
		&lc,
		&bucket.OwnerId,
		&policy,
		&website,
		&encryption,
		&createTime,
		&bucket.Usage,
		&bucket.Versioning,
	)
	if err != nil && err == sql.ErrNoRows {
		err = ErrNoSuchBucket
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
	err = json.Unmarshal([]byte(logging), &bucket.BucketLogging)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(lc), &bucket.Lifecycle)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(policy), &bucket.Policy)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(website), &bucket.Website)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(encryption), &bucket.Encryption)
	if err != nil {
		return
	}
	return
}

func (t *TidbClient) GetBuckets() (buckets []Bucket, err error) {
	sqltext := "select bucketname,acl,cors,COALESCE(logging,\"\"),lc,uid,policy,website,COALESCE(encryption,\"\"),createtime,usages,versioning from buckets;"
	rows, err := t.Client.Query(sqltext)
	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var tmp Bucket
		var acl, cors, logging, lc, policy, website,encryption, createTime string
		err = rows.Scan(
			&tmp.Name,
			&acl,
			&cors,
			&logging,
			&lc,
			&tmp.OwnerId,
			&policy,
			&website,
			&encryption,
			&createTime,
			&tmp.Usage,
			&tmp.Versioning)
		if err != nil {
			return
		}
		tmp.CreateTime, err = time.Parse(TIME_LAYOUT_TIDB, createTime)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(acl), &tmp.ACL)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(cors), &tmp.CORS)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(logging), &tmp.BucketLogging)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(lc), &tmp.Lifecycle)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(policy), &tmp.Policy)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(website), &tmp.Website)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(encryption), &tmp.Encryption)
		if err != nil {
			return
		}
		buckets = append(buckets, tmp)
	}
	return
}

//Actually this method is used to update bucket
func (t *TidbClient) PutBucket(bucket Bucket) error {
	sql, args := bucket.GetUpdateSql()
	_, err := t.Client.Exec(sql, args...)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) CheckAndPutBucket(bucket Bucket) (bool, error) {
	var processed bool
	_, err := t.GetBucket(bucket.Name)
	if err == nil {
		processed = false
		return processed, err
	} else if err != nil && err != ErrNoSuchBucket {
		processed = false
		return processed, err
	} else {
		processed = true
	}
	sql, args := bucket.GetCreateSql()
	_, err = t.Client.Exec(sql, args...)
	return processed, err
}

func (t *TidbClient) ListObjects(bucketName, marker, verIdMarker, prefix, delimiter string, versioned bool, maxKeys int) (retObjects []*Object, prefixes []string, truncated bool, nextMarker, nextVerIdMarker string, err error) {
	if versioned {
		return
	}
	var count int
	var exit bool
	objectNum := make(map[string]int)
	commonPrefixes := make(map[string]struct{})
	omarker := marker
	for {
		var loopcount int
		var sqltext string
		var rows *sql.Rows
		if marker == "" {
			sqltext = "select bucketname,name,version,nullversion,deletemarker from objects where bucketName=? order by bucketname,name,version limit ?;"
			rows, err = t.Client.Query(sqltext, bucketName, maxKeys)
		} else {
			sqltext = "select bucketname,name,version,nullversion,deletemarker from objects where bucketName=? and name >=? order by bucketname,name,version limit ?,?;"
			rows, err = t.Client.Query(sqltext, bucketName, marker, objectNum[marker], objectNum[marker]+maxKeys)
		}
		if err != nil {
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
				_ = rows.Close()
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
			//filte by objectname
			if objectNum[name] > 1 {
				continue
			}
			//filte by deletemarker
			if deletemarker {
				continue
			}
			if name == omarker {
				continue
			}
			//filte by delemiter
			if len(delimiter) != 0 {
				subStr := strings.TrimPrefix(name, prefix)
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := prefix + subStr[0:(n+1)]
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
			var o *Object
			Strver := strconv.FormatUint(version, 10)
			o, err = t.GetObject(bucketname, name, Strver)
			if err != nil {
				_ = rows.Close()
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
		_ = rows.Close()
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
	sqltext := "delete from buckets where bucketname=?;"
	_, err := t.Client.Exec(sqltext, bucket.Name)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) UpdateUsage(bucketName string, size int64, tx DB) (err error) {
	if !helper.CONFIG.PiggybackUpdateUsage {
		return nil
	}

	if tx == nil {
		tx = t.Client
	}
	sql := "update buckets set usages= usages + ? where bucketname=?;"
	_, err = tx.Exec(sql, size, bucketName)
	return
}
