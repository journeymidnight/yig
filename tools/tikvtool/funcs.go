package main

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/journeymidnight/yig/meta/types"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/client/tikvclient"
)

// TODO: unfinished
func SetFunc(key, value string) error {
	c := tikvclient.NewClient(strings.Split(global.PDs, ","))
	var k, v = []byte(key), []byte(value)
	var err error

	if _, ok := TableMap[global.Table]; ok {
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
	c := tikvclient.NewClient(strings.Split(global.PDs, ","))
	var k []byte
	var err error
	if _, ok := TableMap[global.Table]; ok {
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
			printKey := bytes.Replace(k, []byte{0x31}, []byte("\\"), -1)
			fmt.Println("key:", string(printKey))
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
	c := tikvclient.NewClient(strings.Split(global.PDs, ","))
	var prefix string
	var sk, ek []byte
	if _, ok := TableMap[global.Table]; ok {
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
	c := tikvclient.NewClient(strings.Split(global.PDs, ","))
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
	c := tikvclient.NewClient(strings.Split(global.PDs, ","))
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
