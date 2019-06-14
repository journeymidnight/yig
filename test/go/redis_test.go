package _go

import (
	redigo "github.com/gomodule/redigo/redis"
	"testing"
	"time"
)

const (
	RedisInAddress = "localhost:6379"
	RedisPassword  = "hehehehe"
)

func Test_RedisConn(t *testing.T) {
	df := func() (redigo.Conn, error) {
		c, err := redigo.Dial("tcp", RedisInAddress)
		if err != nil {
			return nil, err
		}
		return c, nil
	}

	redisPool := &redigo.Pool{
		MaxIdle:     1,
		IdleTimeout: 1 * time.Second,
		// Other pool configuration not shown in this example.
		Dial: df,
	}
	defer redisPool.Close()

	c := redisPool.Get()
	defer c.Close()
	_, err := c.Do("AUTH", RedisPassword)
	if err != nil {
		t.Fatal("AUTH Cmd err:", err, "Password:", RedisPassword)
	}
	_, err = c.Do("PING")
	if err != nil {
		t.Fatal("PING Cmd err:", err)
	}
	t.Log("Redis ok!")

}
