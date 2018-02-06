package main

import (
	"os"
	"strconv"

	"fmt"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/types"
	yigredis "github.com/journeymidnight/yig/redis"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
)

func main() {

	if len(os.Args) != 3 {
		panic("Usage: getrediskey redis_address:port key")
		return
	}

	var err error
	//initialize
	df := func(network, addr string) (*redis.Client, error) {
		client, err := redis.Dial(network, addr)
		if err != nil {
			return nil, err
		}
		if helper.CONFIG.RedisPassword != "" {
			if err = client.Cmd("AUTH", helper.CONFIG.RedisPassword).Err; err != nil {
				client.Close()
				return nil, err
			}
		}
		return client, nil
	}

	redisConnectionPool, err := pool.NewCustom("tcp", os.Args[1], 1, df)
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
	var tableType yigredis.RedisDatabase
	if t, err := strconv.Atoi(string(key[0])); err == nil {
		tableType = yigredis.RedisDatabase(t)
	} else {
		panic("Failed Get A table type, key value should start with a number")
	}

	encodeValue, err := client.Cmd("GET", key).Bytes()

	if err != nil {
		fmt.Println(err)
		return
	}
	switch tableType {
	case yigredis.BucketTable:
		var v types.Bucket
		err = helper.MsgPackUnMarshal(encodeValue, &v)
		if err != nil {
			fmt.Println("Failed to Unmarshal")
			return
		}
		fmt.Println(v.String())
	case yigredis.ClusterTable:
		var v types.Cluster
		err = helper.MsgPackUnMarshal(encodeValue, &v)
		if err != nil {
			fmt.Println("Failed to Unmarshal")
			return
		}
		fmt.Println(v)
	case yigredis.ObjectTable:
		var v types.Object
		err = helper.MsgPackUnMarshal(encodeValue, &v)
		if err != nil {
			fmt.Println("Failed to Unmarshal")
			return
		}
		fmt.Println(v.String())
	case yigredis.UserTable:
		buckets := make([]string, 0)
		err := helper.MsgPackUnMarshal(encodeValue, &buckets)
		if err != nil {
			fmt.Println("Failed to Unmarshal")
			return
		}
		fmt.Println(buckets)
	case yigredis.FileTable:
		fmt.Println(encodeValue)
	}
}
