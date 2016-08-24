package main

import (
	"git.letv.cn/yig/yig/api"
	"github.com/kataras/iris"
	"log"
)

type adminServerConfig struct {
	Address     string
	Logger      *log.Logger
	ObjectLayer api.ObjectLayer
}

func startAdminServer(config *adminServerConfig) {
	iris.Get("/hi", func(ctx *iris.Context) {
		ctx.Write("Hi %s", "YIG")
	})
	go iris.Listen(config.Address)
}
