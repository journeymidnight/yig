package main

import (
	"git.letv.cn/yig/yig/api"
	"github.com/kataras/iris"
	"log"
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/storage"
)

type adminServerConfig struct {
	Address     string
	Logger      *log.Logger
	ObjectLayer api.ObjectLayer
}

func getUsage(ctx *iris.Context) {
	helper.Logger.Println("enter getusage")
	bucketName := ctx.Param("bucket")
	yig, _ := helper.Yig.(*storage.YigStorage)
	usage, err := yig.MetaStorage.GetUsage(bucketName)
	if err != nil {
		ctx.Write("get usage for bucket:%s failed", bucketName)
		return
	}
	helper.Logger.Println("enter getusage",bucketName,usage)
	ctx.Write("usage for bucket:%s,%d", bucketName, usage)

	return

}
func startAdminServer(config *adminServerConfig) {
	iris.Get("/hi", func(ctx *iris.Context) {
		ctx.Write("Hi %s", "YIG")
	})
	iris.Get("/admin/usage/:bucket", getUsage)
	go iris.Listen(config.Address)
}

func stopAdminServer() {
	// TODO should shutdown admin API server gracefully
}
