package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/client/tikvclient"
	conf "github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	"github.com/tikv/client-go/txnkv"
)

func printKVHelp() {
	fmt.Println("Usage: kvtool <commands> <table> [options...]")
	fmt.Println("Commands: scan")
	fmt.Println("Options:")
	fmt.Println(" -b, --bucket			Bucket name.")
	fmt.Println(" -o, --object			Object name.")
	fmt.Println(" -u, --uid				Specify user name to operate")
	fmt.Println(" -l, --limit			Max scan limit, default is 1000.")
	fmt.Println(" -s, --start-key		Specify scan start key, default is empty string")
	fmt.Println(" -e, --end-key			Specify scan end key.")
	fmt.Println(" -p, --pd				PD address")
}

func GetTableStartKey(table string, args ...string) []byte {
	bucket := args[0]
	object := args[1]
	uid := args[2]
	startKey := args[3]
	endKey := args[4]
	fmt.Println("bucket:", bucket, "user:", uid, "object:", object, "startKey", startKey, "endKey", endKey)
	switch table {
	case "bucket":
		return GenKey(TableBucketPrefix, startKey)
	case "object":
	case "users":
	}
	return nil
}

func GetTableEndKey(table string, args ...string) []byte {
	bucket := args[0]
	object := args[1]
	uid := args[2]
	startKey := args[3]
	endKey := args[4]
	fmt.Println("bucket:", bucket, "user:", uid, "object:", object, "startKey", startKey, "endKey", endKey)
	switch table {
	case "bucket":
		return GenKey(TableBucketPrefix, endKey)
	case "object":
	case "users":
	}
	return nil
}

func main() {
	mySet := flag.NewFlagSet("", flag.ExitOnError)
	if len(os.Args) < 3 {
		printKVHelp()
		return
	}
	command := os.Args[1]
	table := os.Args[2]
	if command == "" || table == "" {
		printKVHelp()
		return
	}

	limit := mySet.Int("l", 1000, "max limit")
	startKey := mySet.String("s", "", "start key")
	endKey := mySet.String("e", string(0xFF), "end key")
	bucket := mySet.String("b", "", "bucket name")
	uid := mySet.String("u", "", "user name")
	object := mySet.String("o", "", "object name")
	pd := mySet.String("p", strings.Join(helper.CONFIG.PdAddress, ","), "pd address")
	mySet.Parse(os.Args[3:])

	c := newClient(strings.Split(*pd, ","))
	switch os.Args[1] {
	case "scan":
		sk := GetTableStartKey(table, *bucket, *object, *uid, *startKey, *endKey)
		ek := GetTableEndKey(table, *bucket, *object, *uid, *startKey, *endKey)
		fmt.Println("Start:", string(sk), "End:", string(ek), "Limit:", *limit)
		kvs, err := c.Scan(sk, ek, *limit)
		if err != nil {
			panic(err)
		}
		fmt.Println("result len:", len(kvs))
		for _, kv := range kvs {
			fmt.Println(string(kv.K))
		}

	default:
		printKVHelp()
		return
	}
}

type myTikvClient struct {
	RawCli *rawkv.Client
	TxnCli *txnkv.Client
}

func newClient(pd []string) *myTikvClient {
	rawCli, err := rawkv.NewClient(context.TODO(), pd, conf.Default())
	if err != nil {
		panic(err)
	}

	txnCli, err := txnkv.NewClient(context.TODO(), pd, conf.Default())
	if err != nil {
		panic(err)
	}
	return &myTikvClient{
		RawCli: rawCli,
		TxnCli: txnCli}
}

func (c *myTikvClient) Scan(startKey []byte, endKey []byte, limit int) ([]KV, error) {
	ks, vs, err := c.RawCli.Scan(context.TODO(), startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	var ret []KV
	for i, k := range ks {
		ret = append(ret, KV{K: k, V: vs[i]})
	}
	return ret, nil
}
