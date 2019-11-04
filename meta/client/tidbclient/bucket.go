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
	var acl, cors, lc, policy, createTime string
	var updateTime sql.NullString
	sqltext := "select bucketname,acl,cors,lc,uid,policy,createtime,usages,versioning,update_time from buckets where bucketname=?;"
	tmp := &Bucket{}
	err = t.Client.QueryRow(sqltext, bucketName).Scan(
		&tmp.Name,
		&acl,
		&cors,
		&lc,
		&tmp.OwnerId,
		&policy,
		&createTime,
		&tmp.Usage,
		&tmp.Versioning,
		&updateTime,
	)
	if err != nil && err == sql.ErrNoRows {
		err = ErrNoSuchBucket
		return
	} else if err != nil {
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
	err = json.Unmarshal([]byte(lc), &tmp.LC)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(policy), &tmp.Policy)
	if err != nil {
		return
	}
	if updateTime.Valid {
		tmp.UpdateTime, err = time.Parse(TIME_LAYOUT_TIDB, updateTime.String)
		if err != nil {
			return
		}
	}
	bucket = tmp
	return
}

func (t *TidbClient) GetBuckets() (buckets []*Bucket, err error) {
	sqltext := "select bucketname,acl,cors,lc,uid,policy,createtime,usages,versioning,update_time from buckets;"
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
		var acl, cors, lc, policy, createTime string
		var updateTime sql.NullString
		err = rows.Scan(
			&tmp.Name,
			&acl,
			&cors,
			&lc,
			&tmp.OwnerId,
			&policy,
			&createTime,
			&tmp.Usage,
			&tmp.Versioning,
			&updateTime)
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
		err = json.Unmarshal([]byte(lc), &tmp.LC)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(policy), &tmp.Policy)
		if err != nil {
			return
		}
		if updateTime.Valid {
			tmp.UpdateTime, err = time.Parse(TIME_LAYOUT_TIDB, updateTime.String)
			if err != nil {
				return
			}
		}
		buckets = append(buckets, &tmp)
	}
	return
}

//Actually this method is used to update bucket
func (t *TidbClient) PutBucket(bucket *Bucket) error {
	sql, args := bucket.GetUpdateSql()
	_, err := t.Client.Exec(sql, args...)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) CheckAndPutBucket(bucket *Bucket) (bool, error) {
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
	const MaxObjectList = 10000
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
		var loopcount int
		var sqltext string
		var rows *sql.Rows
		args := make([]interface{}, 0)
		sqltext = "select bucketname,name,version from objects where bucketName=?"
		args = append(args, bucketName)
		if prefix != "" {
			sqltext += " and name like ?"
			args = append(args, prefix+"%")
			helper.Logger.Printf(20, "query prefix: %s", prefix)
		}
		if marker != "" {
			sqltext += " and name >= ?"
			args = append(args, marker)
			helper.Logger.Printf(20, "query marker: %s", marker)
		}
		if delimiter == "" {
			sqltext += " order by bucketname,name,version limit ?"
			args = append(args, MaxObjectList)
		} else {
			num := len(strings.Split(prefix, delimiter))
			args = append(args, delimiter, num, MaxObjectList)
			sqltext += " group by SUBSTRING_INDEX(name, ?, ?) limit ?"
		}
		tstart := time.Now()
		rows, err = t.Client.Query(sqltext, args...)
		if err != nil {
			return
		}
		tqueryend := time.Now()
		tdur := tqueryend.Sub(tstart).Nanoseconds()
		if tdur/1000000 > 5000 {
			helper.Logger.Printf(5, "slow list objects query: %s,args: %v, takes %d", sqltext, args, tdur)
		}
		helper.Logger.Printf(20, "query sql: %s", sqltext)
		defer rows.Close()
		for rows.Next() {
			loopcount += 1
			//fetch related date
			var bucketname, name string
			var version uint64
			err = rows.Scan(
				&bucketname,
				&name,
				&version,
			)
			if err != nil {
				return
			}
			//prepare next marker
			//TODU: be sure how tidb/mysql compare strings
			if _, ok := objectNum[name]; !ok {
				objectNum[name] = 0
			}
			objectNum[name] += 1
			marker = name

			if _, ok := objectMap[name]; !ok {
				objectMap[name] = struct{}{}
			} else {
				continue
			}
			//filte by deletemarker
			/*if deletemarker {
				continue
			}*/
			if name == omarker {
				continue
			}
			//filte by delemiter
			if len(delimiter) != 0 {
				subStr := strings.TrimPrefix(name, prefix)
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := prefix + string([]byte(subStr)[0:(n+1)])
					marker = prefixKey[0:(len(prefixKey)-1)] + string(delimiter[len(delimiter)-1]+1)
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
				return
			}
			count += 1
			if count == maxKeys {
				nextMarker = name
			}

			if count > maxKeys {
				truncated = true
				exit = true
				break
			}
			retObjects = append(retObjects, o)
		}
		tfor := time.Now()
		tdur = tfor.Sub(tqueryend).Nanoseconds()
		if tdur/1000000 > 5000 {
			helper.Logger.Printf(5, "slow list get objects, takes %d", tdur)
		}
		if loopcount < MaxObjectList {
			exit = true
		}
		if exit {
			break
		}
	}
	prefixes = helper.Keys(commonPrefixes)
	return
}

func (t *TidbClient) DeleteBucket(bucket *Bucket) error {
	sqltext := "delete from buckets where bucketname=?;"
	_, err := t.Client.Exec(sqltext, bucket.Name)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) UpdateUsage(bucketName string, size int64, tx interface{}) (err error) {
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

	sql := "update buckets set usages=? where bucketname=?;"
	_, err = sqlTx.Exec(sql, size, bucketName)
	return
}

func (t *TidbClient) UpdateUsages(usages map[string]int64, tx interface{}) error {
	var sqlTx *sql.Tx
	var err error
	if nil == tx {
		tx, err = t.Client.Begin()
		defer func() {
			if nil == err {
				err = sqlTx.Commit()
			} else {
				sqlTx.Rollback()
			}
		}()
	}
	sqlTx, _ = tx.(*sql.Tx)
	sqlStr := "update buckets set usages = ? where bucketname = ?;"
	st, err := sqlTx.Prepare(sqlStr)
	if err != nil {
		helper.Logger.Println(2, "failed to prepare statment with sql: ", sqlStr, ", err: ", err)
		return err
	}
	defer st.Close()

	for bucket, usage := range usages {
		_, err = st.Exec(usage, bucket)
		if err != nil {
			helper.Logger.Println(2, "failed to update usage for bucket: ", bucket, " with usage: ", usage, ", err: ", err)
			return err
		}
	}
	return nil
}
