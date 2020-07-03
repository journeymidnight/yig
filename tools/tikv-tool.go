/*
	Tikv tool is currently under development and has not been completed.
	Please do not use this tool in the online environment, currently only allowed to run in the test environment.
	If you are interested in completing these codes, please submit your pull request.
*/

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/journeymidnight/yig/meta/types"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/client/tikvclient"

	"github.com/urfave/cli"
)

type Table struct {
	Prefix string
}

var TableMap = map[string]Table{
	"buckets": {
		Prefix: tikvclient.TableBucketPrefix,
	},
	"users": {
		Prefix: tikvclient.TableUserBucketPrefix,
	},
	"objects": {
		Prefix: "",
	},
	"multipart": {
		Prefix: tikvclient.TableMultipartPrefix,
	},
	"part": {
		Prefix: tikvclient.TableObjectPartPrefix,
	},
	"cluster": {
		Prefix: tikvclient.TableClusterPrefix,
	},
	"gc": {
		Prefix: tikvclient.TableGcPrefix,
	},
	"freezer": {
		Prefix: tikvclient.TableFreezerPrefix,
	},
	"hotobjects": {
		Prefix: tikvclient.TableHotObjectPrefix,
	},
	"qos": {
		Prefix: tikvclient.TableQoSPrefix,
	},
}

var table, startKey, endKey string
var maxKeys int

type GlobalOption struct {
	PDs          string
	IsKeyBytes   bool
	IsValueBytes bool
	IsMsgPack    bool
	Table        string
	Bucket       string
	Object       string
	Version      string
}

var global GlobalOption

func main() {
	app := cli.NewApp()
	app.Name = "TiKV Tool"
	app.Usage = "A simple CLI tool to operate tikv for yig."
	app.Version = "0.0.1"
	app.Action = func(c *cli.Context) error {
		cli.ShowAppHelp(cli.NewContext(app, nil, nil))
		return nil
	}

	// Global options
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "pd",
			Value:       "pd1:2378, pd2:2377",
			Usage:       "One or a set of pd addresses. e.g: pd1:2379 or pd1:2379,pd2:2378,pd3:2377",
			Destination: &global.PDs,
		},
		cli.BoolFlag{
			Name:        "keybytes",
			Usage:       "The key is an array of bytes when set or delete. e.g:[1 2 3 4 5]",
			Destination: &global.IsKeyBytes,
		},
		cli.BoolFlag{
			Name:        "valuebytes",
			Usage:       "The value is an array of bytes when set or get. e.g:[1 2 3 4 5]",
			Destination: &global.IsValueBytes,
		},
		cli.BoolFlag{
			Name:        "msgpack",
			Usage:       "Use msgpack to encode(set) or decode(get)",
			Destination: &global.IsMsgPack,
		},
		cli.StringFlag{
			Name:        "table,t",
			Usage:       "Set table prefix",
			Destination: &global.Table,
		},
		cli.StringFlag{
			Name:        "bucket,b",
			Usage:       "Set bucket",
			Destination: &global.Bucket,
		},
		cli.StringFlag{
			Name:        "object,o",
			Usage:       "Set object",
			Destination: &global.Object,
		},
		cli.StringFlag{
			Name:        "versionid,V",
			Usage:       "Set version id",
			Destination: &global.Version,
		},
	}

	app.Commands = []cli.Command{
		{
			Name:  "set",
			Usage: "Set a key",
			Action: func(c *cli.Context) error {
				if len(c.Args()) != 2 {
					cli.ShowCommandHelp(cli.NewContext(app, nil, nil), "set")
					return errors.New("Invalid arguments.")
				}
				return SetFunc(c.Args()[0], c.Args()[1])
			},
			ArgsUsage: "<key> <value>",
		},
		{
			Name:  "get",
			Usage: "Get a key",
			Action: func(c *cli.Context) error {
				if len(c.Args()) != 1 {
					cli.ShowCommandHelp(cli.NewContext(app, nil, nil), "get")
					return errors.New("Invalid arguments.")
				}
				return GetFunc(c.Args()[0])
			},
			ArgsUsage: "<key>",
		},
		{
			Name:  "scan",
			Usage: "Scan keys.",
			Action: func(c *cli.Context) error {
				if len(c.Args()) > 0 {
					cli.ShowCommandHelp(cli.NewContext(app, nil, nil), "scan")
					return errors.New("Invalid arguments.")
				}
				return ScanFunc(startKey, endKey, maxKeys)
			},
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:        "startkey,s",
					Value:       "",
					Usage:       "Start object key",
					Destination: &startKey,
				},
				cli.StringFlag{
					Name:        "endkey,e",
					Value:       "",
					Usage:       "End object key",
					Destination: &endKey,
				},
				cli.IntFlag{
					Name:        "limit,l",
					Value:       1000,
					Usage:       "Max object keys",
					Destination: &maxKeys,
				},
			},
			ArgsUsage: "<table> [options...]",
		},
		{
			Name:  "del",
			Usage: "Delete a key",
			Action: func(c *cli.Context) error {
				if len(c.Args()) != 1 {
					return errors.New("Invalid arguments.")
				}
				return DelFunc(c.Args()[0])
			},
			ArgsUsage: "<key>",
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

// TODO: unfinished
func SetFunc(key, value string) error {
	c := tikvclient.NewClient(strings.Split(global.PDs, ","))
	var k, v = []byte(key), []byte(value)
	var err error
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
	c := tikvclient.NewClient(strings.Split(global.PDs, ","))
	var prefix string
	var sk, ek []byte
	if _, ok := TableMap[global.Table]; ok {
		if TableMap[global.Table].Prefix != "" {
			prefix = TableMap[global.Table].Prefix + tikvclient.TableSeparator
		} else {
			if global.Bucket == "" {
				return fmt.Errorf("You must need to specify target bucket. ")
			}
			prefix = global.Bucket + tikvclient.TableSeparator
		}
		if startKey != "" && !strings.HasPrefix(startKey, prefix) {
			return fmt.Errorf("Invalid startKey %s or table %s. ", startKey, table)
		}
		if endKey != "" && !strings.HasPrefix(endKey, prefix) {
			return fmt.Errorf("Invalid endKey %s or table %s. ", endKey, table)
		}
		if startKey == "" {
			startKey = prefix + tikvclient.TableMinKeySuffix
		}
		if endKey == "" {
			endKey = prefix + tikvclient.TableMaxKeySuffix
		} else if strings.Index(endKey, "$") != -1 {
			endKey = strings.ReplaceAll(endKey, "$", string(tikvclient.TableMaxKeySuffix))
		}
	}

	sk, ek = []byte(startKey), []byte(endKey)
	fmt.Println("Table:", global.Table, "Start:", string(sk), "End:", string(ek), "Limit:", maxKeys)
	kvs, err := c.TxScan(sk, ek, maxKeys, nil)
	if err != nil {
		panic(err)
	}

	for _, kv := range kvs {
		fmt.Println("Key:", string(kv.K))
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

func Decode(table string, data []byte) (string, error) {
	if _, ok := TableMap[global.Table]; !ok {
		return string(data), nil
	}
	var v interface{}
	switch table {
	case "buckets":
		var b types.Bucket
		v = b
	case "users":
		var bu tikvclient.BucketUsage
		v = bu
	case "objects":
		fallthrough
	case "hotobjects":
		var o types.Object
		v = o
	}
	err := helper.MsgPackUnMarshal(data, &v)
	if err != nil {
		return "", err
	}
	d, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(d), nil
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
	fmt.Println("Delete key", string(k), "success.")
	return nil
}

func ParseToBytes(s string) (bs []byte, err error) {
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	ss := strings.Split(s, " ")
	for _, v := range ss {
		i, err := strconv.Atoi(v)
		if err != nil {
			return nil, err
		}
		if i < 0 || i > 255 {
			return nil, errors.New("Invalid bytes")
		}
		bs = append(bs, byte(i))
	}
	return
}
