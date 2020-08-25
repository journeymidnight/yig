/*
	Tikv tool is currently under development and has not been completed.
	Please do not use this tool in the online environment, currently only allowed to run in the test environment.
	If you are interested in completing these codes, please submit your pull request.
*/

package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"

	"github.com/journeymidnight/yig/meta/types"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/client/tikvclient"

	"github.com/urfave/cli"
)

var table, startKey, endKey string
var maxKeys int

type GlobalOption struct {
	PDs          string
	Verbose      bool
	IsKeyBytes   bool
	IsValueBytes bool
	IsMsgPack    bool
	Table        string
	Bucket       string
	Object       string
	Version      string
	Args         cli.StringSlice // Format is "k1=v1,k2=v2,k3=v3..."
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
			Name:        "verbose, z",
			Usage:       "verbose option",
			Destination: &global.Verbose,
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
		cli.StringSliceFlag{
			Name:  "args",
			Usage: "set args",
			Value: &global.Args,
		},
	}

	app.Commands = []cli.Command{
		{
			Name:  "set",
			Usage: "Set key and value",
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
		{
			Name:  "drop",
			Usage: "drop a table",
			Action: func(c *cli.Context) error {
				return DropFunc()
			},
		},
		{
			Name:  "repair",
			Usage: "repair the usage of specified bucket",
			Action: func(c *cli.Context) error {
				if len(c.Args()) != 1 {
					return errors.New("Invalid arguments.")
				}
				return RepairFunc(c.Args()[0])
			},
			ArgsUsage: "Repair the data before this hour. e.g. 2020082012",
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func Decode(table string, data []byte) (string, error) {
	if _, ok := TableMap[global.Table]; !ok {
		return string(data), nil
	}
	var v interface{}
	switch table {
	case TableBuckets:
		var b types.Bucket
		v = b
	case TableClusters:

	case TableMultiParts:

	case TableParts:

	case TableRestore:
		var r types.Freezer
		v = r
	case TableUsers:
		var bu tikvclient.BucketUsage
		v = bu
	case TableObjects:
		fallthrough
	case TableHotObjects:
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
