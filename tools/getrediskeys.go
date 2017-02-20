package main

import (
	"os"
	"strconv"

	"legitlab.letv.cn/yig/yig/redis"

	"fmt"

	"github.com/mediocregopher/radix.v2/pool"
	"legitlab.letv.cn/yig/yig/helper"
	"legitlab.letv.cn/yig/yig/meta"
)

func main() {

	if len(os.Args) != 3 {
		panic("Usage: getrediskey redis_address:port key")
		return
	}

	var err error
	//initialize
	redisConnectionPool, err := pool.New("tcp", os.Args[1], 1)
	if err != nil {
		panic("Failed to connect to Redis server: " + err.Error())
	}
	defer redisConnectionPool.Empty()

	client, err := redisConnectionPool.Get()
	if err != nil {
		panic("Failed to get a redis client")
	}
	defer redisConnectionPool.Put(client)

	key := os.Args[2]
	//parse table name
	var tableType redis.RedisDatabase
	if t, err := strconv.Atoi(string(key[0])); err == nil {
		tableType = redis.RedisDatabase(t)
	} else {
		panic("Failed Get A table type, key value should start with a number")
	}

	encodeValue, err := client.Cmd("GET", key).Bytes()

	if err != nil {
		fmt.Println(err)
		return
	}
	switch tableType {
	case redis.BucketTable:
		var v meta.Bucket
		err = helper.MsgPackUnMarshal(encodeValue, &v)
		if err != nil {
			fmt.Println("Failed to Unmarshal")
			return
		}
		fmt.Println(v.String())
	case redis.ClusterTable:
		var v meta.Cluster
		err = helper.MsgPackUnMarshal(encodeValue, &v)
		if err != nil {
			fmt.Println("Failed to Unmarshal")
			return
		}
		fmt.Println(v)
	case redis.ObjectTable:
		var v meta.Object
		err = helper.MsgPackUnMarshal(encodeValue, &v)
		if err != nil {
			fmt.Println("Failed to Unmarshal")
			return
		}
		fmt.Println(v.String())
	case redis.UserTable:
		buckets := make([]string, 0)
		err := helper.MsgPackUnMarshal(encodeValue, &buckets)
		if err != nil {
			fmt.Println("Failed to Unmarshal")
			return
		}
		fmt.Println(buckets)
	case redis.FileTable:
		fmt.Println(encodeValue)
	}
}
