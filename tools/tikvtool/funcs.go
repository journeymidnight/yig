package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/journeymidnight/yig/meta/client/tidbclient"

	"github.com/journeymidnight/yig/meta/common"

	"github.com/journeymidnight/yig/meta/types"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/client/tikvclient"
)

var reflectMap map[string]string
var c *tikvclient.TiKVClient

// TODO: unfinished
func SetFunc(key, value string) error {
	c = tikvclient.NewClient(strings.Split(global.PDs, ","))
	defer c.TxnCli.Close()
	var k, v = []byte(key), []byte(value)
	var err error

	if t, ok := TableMap[global.Table]; ok && t.ExistInTiKV {
		fmt.Println("Args:", global.Args.Value())
		var argMap = make(map[string]string)
		for _, v := range global.Args {
			sp := strings.Split(v, ",")
			for _, v2 := range sp {
				sp2 := strings.SplitN(v2, "=", 2)
				if len(sp) < 2 {
					return fmt.Errorf("invalid args format: %s", global.Args)
				}
				argMap[sp2[0]] = sp2[1]
			}
		}
		switch global.Table {
		case TableClusters:
			// Key: c\{PoolName}\{Fsid}\{Backend}
			k = tikvclient.GenKey(tikvclient.TableClusterPrefix, argMap["pool"], argMap["fsid"], argMap["backend"])
			weight, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid value: %s for table %s", value, global.Table)
			}
			v, err = helper.MsgPackMarshal(weight)
			if err != nil {
				return fmt.Errorf("MsgPackMarshal err: %s", err)
			}
		}
		fmt.Println("Put:", string(k), value)
		return c.TxPut(k, v)
	} else if global.Table != "" {
		return fmt.Errorf("invalid table name: %s", global.Table)
	}

	if global.IsKeyBytes {
		k, err = ParseToBytes(key)
		if err != nil {
			return err
		}
	}
	if global.IsValueBytes {
		v, err = ParseToBytes(value)
		if err != nil {
			return err
		}
	}

	if global.IsMsgPack {
		v, err = helper.MsgPackMarshal(v)
		if err != nil {
			return err
		}
	}
	return c.TxPut(k, v)
}

// TODO: unfinished
func GetFunc(key string) error {
	c = tikvclient.NewClient(strings.Split(global.PDs, ","))
	defer c.TxnCli.Close()
	var k []byte
	var err error
	if t, ok := TableMap[global.Table]; ok && t.ExistInTiKV {
		fmt.Println("Args:", global.Args.Value())
		var argMap = make(map[string]string)
		for _, v := range global.Args {
			sp := strings.Split(v, ",")
			for _, v2 := range sp {
				sp2 := strings.SplitN(v2, "=", 2)
				if len(sp) < 2 {
					return fmt.Errorf("invalid args format: %s", global.Args)
				}
				argMap[sp2[0]] = sp2[1]
			}
		}
		switch global.Table {
		case TableObjects:
			if global.Version == "" {
				k = tikvclient.GenKey(global.Bucket, key)
			} else {
				k = tikvclient.GenKey(global.Bucket, key, global.Version)
			}
			fmt.Println("key:", string(DecodeKey(k)))
			var o types.Object
			ok, err := c.TxGet(k, &o, nil)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("no such key")
			}
			fmt.Printf("val: %+v \n", o)

		case TableClusters:

		}

	} else if global.Table != "" {
		return fmt.Errorf("invalid table name: %s", global.Table)
	}

	if global.IsKeyBytes {
		k, err = ParseToBytes(key)
		if err != nil {
			return err
		}
	} else {
		k = []byte(key)
	}

	val, err := c.TxGetPure(k, nil)
	if err != nil {
		return err
	}
	// TODO: add type transfer by table
	fmt.Println(string(val))
	return nil
}

func ScanFunc(startKey, endKey string, maxKeys int) (err error) {
	startKey = strings.Replace(startKey, "\\", tikvclient.TableSeparator, -1)
	endKey = strings.Replace(endKey, "\\", tikvclient.TableSeparator, -1)
	c = tikvclient.NewClient(strings.Split(global.PDs, ","))
	defer c.TxnCli.Close()
	var prefix string
	var sk, ek []byte
	if t, ok := TableMap[global.Table]; ok && t.ExistInTiKV {
		if TableMap[global.Table].Prefix != "" {
			prefix = TableMap[global.Table].Prefix + tikvclient.TableSeparator
		} else {
			if global.Bucket == "" {
				return fmt.Errorf("you must specify target bucket")
			}
			prefix = global.Bucket + tikvclient.TableSeparator
		}
		if startKey != "" && !strings.HasPrefix(startKey, prefix) {
			return fmt.Errorf("Invalid startKey %s or table %s ", startKey, table)
		}
		if endKey != "" && !strings.HasPrefix(endKey, prefix) {
			return fmt.Errorf("Invalid endKey %s or table %s ", endKey, table)
		}
		if startKey == "" {
			startKey = prefix + tikvclient.TableMinKeySuffix
		}
		if endKey == "" {
			endKey = prefix + tikvclient.TableMaxKeySuffix
		} else if strings.Index(endKey, "$") != -1 {
			endKey = strings.ReplaceAll(endKey, "$", tikvclient.TableMaxKeySuffix)
		}
	} else if global.Table != "" {
		return fmt.Errorf("invalid table name: %s", global.Table)
	}

	sk, ek = []byte(startKey), []byte(endKey)
	fmt.Println("Table:", global.Table, "Start:", FormatKey(sk), "End:", FormatKey(ek), "Limit:", maxKeys)
	kvs, err := c.TxScan(sk, ek, maxKeys, nil)
	if err != nil {
		panic(err)
	}

	for _, kv := range kvs {
		fmt.Println("Key:", FormatKey(kv.K))
		v, err := Decode(global.Table, kv.V)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			fmt.Println(v)
		}
		fmt.Println("----------------")
	}
	return nil
}

func DelFunc(key string) error {
	c = tikvclient.NewClient(strings.Split(global.PDs, ","))
	defer c.TxnCli.Close()
	var k []byte
	var err error
	if global.IsKeyBytes {
		k, err = ParseToBytes(key)
		if err != nil {
			return err
		}
	} else {
		k = []byte(key)
	}

	err = c.TxDelete(k)
	if err != nil {
		return err
	}
	fmt.Println("Delete key", FormatKey(k), "success.")
	return nil
}

func DropFunc() error {
	table := global.Table
	if table == "" {
		return errors.New("you must specify target table")
	}
	if _, ok := TableMap[table]; !ok {
		return errors.New("invalid table name")
	}

	var confirm string
	fmt.Println("Are you sure to drop the table", table, "? If you confirm, please enter yes-i-really-mean-it")
	fmt.Scan(&confirm)
	if confirm != "yes-i-really-mean-it" {
		fmt.Println("invalid input")
		return nil
	}
	c = tikvclient.NewClient(strings.Split(global.PDs, ","))
	defer c.TxnCli.Close()
	var prefix string
	if table == TableObjects {
		if global.Bucket == "" {
			return errors.New("you must specify target bucket")
		}
		prefix = global.Bucket + tikvclient.TableSeparator
	} else {
		prefix = TableMap[global.Table].Prefix + tikvclient.TableSeparator
	}
	startKey := prefix + tikvclient.TableMinKeySuffix
	endKey := prefix + tikvclient.TableMaxKeySuffix
	sk, ek := []byte(startKey), []byte(endKey)
	fmt.Println(startKey, endKey)
	var count int
	for {
		n, err := c.TxDeleteRange(sk, ek, 1000, nil)
		if err != nil {
			panic(err)
		}
		count += n
		if n == 0 {
			break
		}
	}
	fmt.Println("Delete key count:", count)
	return nil
}

func RepairFunc(hourKey string) (err error) {
	if global.Bucket == "" {
		fmt.Println("You must specified a bucket.")
		return nil
	}

	repairTime, err := time.ParseInLocation("2006010215", hourKey, time.Local)
	if err != nil {
		return
	}
	repairTs := repairTime.UnixNano()
	c = tikvclient.NewClient(strings.Split(global.PDs, ","))
	defer c.TxnCli.Close()
	fmt.Println("====== Start Repair Bucket :", global.Bucket, "======")
	fmt.Println("Repair Time:", repairTime)
	fmt.Println("Repair TimeStamp:", repairTs)
	startKey := tikvclient.GenKey(global.Bucket, tikvclient.TableMinKeySuffix)
	endKey := tikvclient.GenKey(global.Bucket, tikvclient.TableMaxKeySuffix)

	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			err = tx.Commit(context.Background())
		}
		if err != nil {
			tx.Rollback()
		}
	}()

	fmt.Println("Check bucket...")
	bucketKey := tikvclient.GenKey(tikvclient.TableBucketPrefix, global.Bucket)
	var b types.Bucket
	ok, err := c.TxGet(bucketKey, &b, tx)
	if err != nil {
		return
	}
	if !ok {
		fmt.Println("No such bucket:", global.Bucket)
		return
	}
	fmt.Println("Check bucket ok!")
	it, err := tx.Iter(context.TODO(), startKey, endKey)
	if err != nil {
		return err
	}
	defer it.Close()

	var o_standard, o_standardIa, o_glacier int64
	fmt.Println("Scan object...")

	// scan object
	for it.Valid() {
		k, v := it.Key(), it.Value()
		var o types.Object
		err := helper.MsgPackUnMarshal(v, &o)
		printKey := string(EncodeKey(k))
		if err != nil {
			fmt.Println("MsgPackUnMarshal err:", err, "key:", printKey)
			break
		}
		if o.CreateTime > uint64(repairTs) {
			if err := it.Next(context.TODO()); err != nil && it.Valid() {
				return err
			}
			continue
		}
		fmt.Println("Key:", printKey, "StorageClass:", o.StorageClass.ToString(), "Size:", o.Size, "CTime:", o.CreateTime)
		switch o.StorageClass {
		case common.ObjectStorageClassStandard:
			o_standard += o.Size
		case common.ObjectStorageClassStandardIa:
			o_standardIa += common.CorrectDeltaSize(o.StorageClass, o.Size)
		case common.ObjectStorageClassGlacier:
			o_glacier += common.CorrectDeltaSize(o.StorageClass, o.Size)
		}

		if err := it.Next(context.TODO()); err != nil && it.Valid() {
			return err
		}
	}
	fmt.Println("Scan object result: Standard:", o_standard, "StandardIA:", o_standardIa, "Glacier:", o_glacier)

	var p_standard, p_standardIa, p_glacier int64
	startKey = tikvclient.GenKey(tikvclient.TableMultipartPrefix, global.Bucket, tikvclient.TableMinKeySuffix)
	endKey = tikvclient.GenKey(tikvclient.TableMultipartPrefix, global.Bucket, tikvclient.TableMaxKeySuffix)
	fmt.Println("Scan multipart...")
	it, err = tx.Iter(context.TODO(), startKey, endKey)
	if err != nil {
		return
	}

	// scan multipart
	for it.Valid() {
		k, v := it.Key(), it.Value()
		var m types.Multipart
		err := helper.MsgPackUnMarshal(v, &m)
		printKey := string(EncodeKey(k))
		if err != nil {
			fmt.Println("MsgPackUnMarshal err:", err, "key:", printKey)
			break
		}
		startPartKey := tikvclient.GenKey(tikvclient.TableObjectPartPrefix, global.Bucket, m.ObjectName, tikvclient.TableMinKeySuffix)
		endPartKey := tikvclient.GenKey(tikvclient.TableObjectPartPrefix, global.Bucket, m.ObjectName, tikvclient.TableMaxKeySuffix)
		it2, err := tx.Iter(context.TODO(), startPartKey, endPartKey)
		if err != nil {
			return err
		}
		for it2.Valid() {
			k2, v2 := it2.Key(), it2.Value()
			var p types.Part
			err := helper.MsgPackUnMarshal(v2, &p)
			printKey := string(EncodeKey(k2))
			if err != nil {
				fmt.Println("MsgPackUnMarshal err:", err, "key:", printKey)
				break
			}

			partCTime, err := time.ParseInLocation(types.CREATE_TIME_LAYOUT, p.LastModified, time.Local)
			if err != nil {
				return err
			}
			partCTs := partCTime.UnixNano()
			if partCTs > repairTs {
				if err := it2.Next(context.TODO()); err != nil && it2.Valid() {
					return err
				}
				continue
			}

			fmt.Println("Key:", printKey, "StorageClass:", m.Metadata.StorageClass.ToString(), "PartNumber:", p.PartNumber, "Size:", p.Size, "CTime:", partCTs)
			switch m.Metadata.StorageClass {
			case common.ObjectStorageClassStandard:
				p_standard += p.Size
			case common.ObjectStorageClassStandardIa:
				p_standardIa += common.CorrectDeltaSize(m.Metadata.StorageClass, p.Size)
			case common.ObjectStorageClassGlacier:
				p_glacier += common.CorrectDeltaSize(m.Metadata.StorageClass, p.Size)
			}
			if err := it2.Next(context.TODO()); err != nil && it2.Valid() {
				return err
			}
		}

		if err := it.Next(context.TODO()); err != nil && it.Valid() {
			return err
		}
	}

	fmt.Println("Scan multipart result: Standard:", p_standard, "StandardIA:", p_standardIa, "Glacier:", p_glacier)
	standard := o_standard + p_standard
	standardIa := o_standardIa + p_standardIa
	glacier := o_glacier + p_glacier
	fmt.Println("Sum result of bucket: ", b.Name, "Standard:", standard, "StandardIA:", standardIa, "Glacier:", glacier)

	// Update TiKV
	if !global.Verbose {
		key := GenUserBucketKey(b.OwnerId, b.Name)
		val := tikvclient.BucketUsage{
			Standard:   standard,
			StandardIa: standardIa,
			Glacier:    glacier,
		}
		v, err := helper.MsgPackMarshal(val)
		if err != nil {
			return err
		}
		err = tx.Set(key, v)
		if err != nil {
			return err
		}
	}

	fmt.Println("Finished")
	return
}

func GenUserBucketKey(ownerId, bucketName string) []byte {
	return tikvclient.GenKey(tikvclient.TableUserBucketPrefix, ownerId, bucketName)
}

// projectId -> userId
func LoadPidToUidMap(mapPath string) map[string]string {
	f, err := os.Open(mapPath)
	if err != nil {
		fmt.Println("Cannot open file", global.MigrateMapFile, err)
		os.Exit(1)
	}
	m := make(map[string]string)
	buf := bufio.NewReader(f)
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		sp := strings.Split(line, " ")
		if len(sp) != 2 {
			fmt.Println("invalid line: " + line)
			os.Exit(1)
		}
		//projectId -> userId
		sp[0] = strings.TrimSpace(sp[0])
		sp[1] = strings.TrimSpace(sp[1])
		m[sp[0]] = sp[1]
		fmt.Println("Load projectId:", sp[0], "userId:", sp[1])
	}
	return m
}

func MigrateFunc() (err error) {
	reflectMap = LoadPidToUidMap(global.MigrateMapFile)
	if global.TidbAddr == "" {
		return errors.New("no tidb address set")
	}

	if global.Bucket == "" {
		return errors.New("no bucket set")
	}

	// New Tidb
	tidbCli := &tidbclient.TidbClient{}
	conn, err := sql.Open("mysql", global.TidbAddr)
	if err != nil {
		return err
	}
	conn.SetMaxIdleConns(10)
	conn.SetMaxOpenConns(100)
	conn.SetConnMaxLifetime(time.Duration(30) * time.Second)
	tidbCli.Client = conn

	// New Tikv
	c = tikvclient.NewClient(strings.Split(global.PDs, ","))
	defer c.TxnCli.Close()
	// Migrate bucket
	b, err := tidbCli.GetBucket(global.Bucket)
	if err != nil {
		return err
	}
	bByte, _ := json.Marshal(b)
	fmt.Println(fmt.Sprintf("BucketInfo: %s", string(bByte)))

	userId := reflectMap[b.OwnerId]
	if userId == "" {
		return fmt.Errorf("no such userId of projectId: %s", b.OwnerId)
	}

	if !global.Verbose {
		b.OwnerId = userId
		err = c.PutNewBucket(*b)
		if err != nil {
			fmt.Println("PutNewBucket err:", err)
			return err
		}
	}

	// Migrate objects by bucket
	objects, err := GetObjectsByBucket(tidbCli, global.Bucket)
	if err != nil {
		fmt.Println("GetObjectsByBucket err:", err)
		return err
	}
	fmt.Println("Total objects count:", len(objects), "of bucket:", global.Bucket)

	for _, object := range objects {
		ob, _ := json.Marshal(object)
		fmt.Println(string(ob))
		if !global.Verbose {
			object.OwnerId = userId
			err = c.PutObject(object, nil, false)
			if err != nil {
				fmt.Println("Put object err:", err, "of bucket:", global.Bucket)
				return err
			}
		}
	}
	return nil
}

func ConvertFunc() (err error) {
	reflectMap = LoadPidToUidMap(global.MigrateMapFile)
	// Check export dir
	if exportDir == "" {
		fmt.Println("please specify the export directory by using -e or --export after `parse`.")
		os.Exit(1)
	}
	ok, err := PathExists(exportDir)
	if err != nil {
		fmt.Println("open path", exportDir, "err:", err)
		os.Exit(1)
	}
	if !ok {
		fmt.Println("path", exportDir, "not exist")
		os.Exit(1)
	}

	// specify the tables you wanna parse
	// NOTE: TableObjectPart and TableRestoreObjectPart should be parsed first
	var tables []string
	if global.Table != "" {
		if _, ok := TableMap[global.Table]; !ok {
			fmt.Println("table name", global.Table, "is invalid")
			os.Exit(1)
		}
		if global.Table == TableObjects {
			tables = []string{TableObjectPart, TableObjects}
		} else if global.Table == TableRestore {
			tables = []string{TableRestoreObjectPart, TableRestore}
		} else {
			tables = []string{global.Table}
		}
	} else {
		tables = []string{
			TableBuckets, TableUsers, TableObjectPart, TableObjects, TableMultiParts, TableParts, TableClusters, TableHotObjects,
			TableRestoreObjectPart, TableRestore, TableLifeCycle, TableQos,
		}
	}

	c = tikvclient.NewClient(strings.Split(global.PDs, ","))
	defer c.TxnCli.Close()

	for _, table := range tables {
		ConvertByDMLFile(exportDir, database, table)
	}

	return nil
}
