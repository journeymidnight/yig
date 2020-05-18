package main

import (
	"fmt"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/types"
	yigredis "github.com/journeymidnight/yig/redis"
	"os"
	"strconv"
	"time"
)

func main() {

	if len(os.Args) != 3 {
		panic("Usage: getrediskey redis_address:port key")
	}

	var err error
	//initialize
	timeout := 1 * time.Second
	options := []redigo.DialOption{
		redigo.DialReadTimeout(timeout),
		redigo.DialConnectTimeout(timeout),
		redigo.DialWriteTimeout(timeout),
	}

	redisConnectionPool := &redigo.Pool{
		MaxIdle:     5,
		IdleTimeout: 30 * time.Second,

		// Other pool configuration not shown in this example.
		Dial: func() (redigo.Conn, error) {
			c, err := redigo.Dial("tcp", os.Args[1], options...)
			if err != nil {
				return nil, err
			}
			if helper.CONFIG.RedisPassword != "" {
				if _, err := c.Do("AUTH", helper.CONFIG.RedisPassword); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, nil
		},
	}

	client := redisConnectionPool.Get()
	client.Close()

	key := os.Args[2]
	//parse table name
	var tableType yigredis.RedisDatabase
	if t, err := strconv.Atoi(string(key[0])); err == nil {
		tableType = yigredis.RedisDatabase(t)
	} else {
		panic("Failed Get A table type, key value should start with a number")
	}

	encodeValue, err := redigo.Bytes(client.Do("GET", key))

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
