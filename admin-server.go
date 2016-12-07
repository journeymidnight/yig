package main

import (
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/storage"
	"git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/iam"
	"github.com/kataras/iris"
	"log"
	"net/http"
)

type adminServerConfig struct {
	Address string
	Logger  *log.Logger
	Yig     *storage.YigStorage
}

type userJson struct {
	Buckets []string
	Keys []iam.AccessKeyItem
}

type cacheJson struct {
	HitRate float64
}

var adminServer *adminServerConfig

func getUsage(ctx *iris.Context) {
	helper.Debugln("enter getusage")
	bucketName := ctx.URLParam("bucket")
	usage, err := adminServer.Yig.MetaStorage.GetUsage(bucketName)
	if err != nil {
		ctx.Write("get usage for bucket:%s failed", bucketName)
		return
	}
	helper.Debugln("enter getusage", bucketName, usage)
	ctx.Write("usage for bucket:%s,%d", bucketName, usage)

	return
}

func getBucketInfo(ctx *iris.Context) {
	helper.Debugln("enter getBucketInfo")
	bucketName := ctx.URLParam("bucket")
	bucket, err := adminServer.Yig.MetaStorage.GetBucketInfo(bucketName)
	if err != nil {
		var status int
		apiErrorCode, ok := err.(error.ApiError)
		if ok {
			status = apiErrorCode.HttpStatusCode()
		} else {
			status = http.StatusInternalServerError
		}
		ctx.JSON(status, "")
		return
	}
	ctx.JSON(iris.StatusOK, bucket)
	return
}

func getUserInfo(ctx *iris.Context) {
	helper.Debugln("enter getUserInfo")
	uid := ctx.URLParam("uid")
	buckets, err := adminServer.Yig.MetaStorage.GetUserInfo(uid)
	if err != nil {
		var status int
		apiErrorCode, ok := err.(error.ApiError)
		if ok {
			status = apiErrorCode.HttpStatusCode()
		} else {
			status = http.StatusInternalServerError
		}
		ctx.EmitError(status)
		return
	}
	helper.Debugln("enter getUserInfo", uid, buckets)

	keys, err := iam.GetKeysByUid(uid)
	if err != nil {
		var status int

		apiErrorCode, ok := err.(error.ApiError)
		if ok {
			status = apiErrorCode.HttpStatusCode()
		} else {
			status = http.StatusInternalServerError
		}
		ctx.EmitError(status)
		return
	}
	ctx.JSON(iris.StatusOK, userJson{Buckets: buckets,Keys:keys})

	return
}

func getObjectInfo(ctx *iris.Context) {
	helper.Debugln("enter getObjectInfo")
	bucketName := ctx.URLParam("bucket")
	objectName := ctx.URLParam("object")
	object, err := adminServer.Yig.MetaStorage.GetObject(bucketName, objectName)
	if err != nil {
		var status int

		apiErrorCode, ok := err.(error.ApiError)
		if ok {
			status = apiErrorCode.HttpStatusCode()
		} else {
			status = http.StatusInternalServerError
		}
		ctx.EmitError(status)
		return
	}
	ctx.JSON(iris.StatusOK, object)
	return
}

func getCacheHitRate(ctx *iris.Context) {
	helper.Debugln("enter getCacheHitRate")
	hit := adminServer.Yig.MetaStorage.Cache.Hit
	miss := adminServer.Yig.MetaStorage.Cache.Miss
	rate := float64(hit)/float64(hit+miss)
	ctx.JSON(iris.StatusOK, cacheJson{HitRate:rate})
	return
}

func startAdminServer(config *adminServerConfig) {
	adminServer = config
	iris.Get("/hi", func(ctx *iris.Context) {
		ctx.Write("Hi %s", "YIG")
	})
	iris.Get("/admin/usage", getUsage)
	iris.Get("/admin/user", getUserInfo)
	iris.Get("/admin/bucket", getBucketInfo)
	iris.Get("/admin/object", getObjectInfo)
	iris.Get("/admin/cachehit", getCacheHitRate)
	go iris.Listen(config.Address)
}

func stopAdminServer() {
	// TODO should shutdown admin API server gracefully
}
