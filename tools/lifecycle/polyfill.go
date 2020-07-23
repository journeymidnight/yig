package main

import (
	"github.com/go-redis/redis/v7"
	"time"
)

const (
	maxRetries   = 10
	dialTimeout  = time.Second
	readTimeout  = time.Second
	writeTimeout = time.Second
	idleTimeout  = 30 * time.Second
)

func NewRedisClient(addresses []string, password string) redis.UniversalClient {
	options := redis.UniversalOptions{
		MaxRetries:   maxRetries,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
		Addrs:        addresses,
		Password:     password,
	}
	return redis.NewUniversalClient(&options)
}
