package main

import (
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/storage"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/iam"
	"github.com/kataras/iris"
	"log"
	"net/http"
	"github.com/dgrijalva/jwt-go"
	jwtmiddleware "github.com/iris-contrib/middleware/jwt"
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

type usageJson struct {
	Usage int64
}

var adminServer *adminServerConfig
var myJwtMiddleware *jwtmiddleware.Middleware

func getUsage(ctx *iris.Context) {
	helper.Debugln("enter getusage")
	var bucketName string
	userToken := myJwtMiddleware.Get(ctx)
	if claims, ok := userToken.Claims.(jwt.MapClaims); ok && userToken.Valid {
		bucketName = claims["bucket"].(string)
	} else {
		ctx.EmitError(400)
		return
	}

	usage, err := adminServer.Yig.MetaStorage.GetUsage(bucketName)
	if err != nil {
		ctx.Write("get usage for bucket:%s failed", bucketName)
		return
	}
	helper.Debugln("enter getusage", bucketName, usage)
	ctx.JSON(iris.StatusOK, usageJson{Usage:usage})
	return
}



func getBucketInfo(ctx *iris.Context) {

	helper.Debugln("enter getBucketInfo")
	var bucketName string

	userToken := myJwtMiddleware.Get(ctx)
	if claims, ok := userToken.Claims.(jwt.MapClaims); ok && userToken.Valid {
		bucketName = claims["bucket"].(string)
	} else {
		ctx.EmitError(400)
		return
	}

	helper.Debugln("bucketName:", bucketName)
	bucket, err := adminServer.Yig.MetaStorage.GetBucketInfo(bucketName)
	if err != nil {
		var status int
		apiErrorCode, ok := err.(ApiError)
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
	var uid string
	userToken := myJwtMiddleware.Get(ctx)
	if claims, ok := userToken.Claims.(jwt.MapClaims); ok && userToken.Valid {
		uid = claims["uid"].(string)
	} else {
		ctx.EmitError(400)
		return
	}

	buckets, err := adminServer.Yig.MetaStorage.GetUserInfo(uid)
	if err != nil {
		var status int
		apiErrorCode, ok := err.(ApiError)
		if ok {
			status = apiErrorCode.HttpStatusCode()
		} else {
			status = http.StatusInternalServerError
		}
		ctx.EmitError(status)
		return
	}
	helper.Debugln("enter getUserInfo", uid, buckets)

	var keys []iam.AccessKeyItem
	if helper.CONFIG.DebugMode == false {
		keys, err = iam.GetKeysByUid(uid)
		if err != nil {
			var status int

			apiErrorCode, ok := err.(ApiError)
			if ok {
				status = apiErrorCode.HttpStatusCode()
			} else {
				status = http.StatusInternalServerError
			}
			ctx.EmitError(status)
			return
		}
	}
	ctx.JSON(iris.StatusOK, userJson{Buckets: buckets,Keys:keys})

	return
}

func getObjectInfo(ctx *iris.Context) {
	helper.Debugln("enter getObjectInfo")
	var bucketName string
	var objectName string

	userToken := myJwtMiddleware.Get(ctx)
	if claims, ok := userToken.Claims.(jwt.MapClaims); ok && userToken.Valid {
		bucketName = claims["bucket"].(string)
		objectName = claims["object"].(string)
	} else {
		ctx.EmitError(400)
		return
	}

	object, err := adminServer.Yig.MetaStorage.GetObject(bucketName, objectName)
	if err != nil {
		var status int

		apiErrorCode, ok := err.(ApiError)
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
	userToken := myJwtMiddleware.Get(ctx)
	if userToken.Valid == false {
		ctx.EmitError(403)
		return
	}

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

	myJwtMiddleware = jwtmiddleware.New(jwtmiddleware.Config{
		ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) {
			return []byte(helper.CONFIG.AdminKey), nil
		},
		SigningMethod: jwt.SigningMethodHS256,
	})

	iris.Get("/admin/usage", myJwtMiddleware.Serve, getUsage)
	iris.Get("/admin/user", myJwtMiddleware.Serve, getUserInfo)
	iris.Get("/admin/bucket", myJwtMiddleware.Serve, getBucketInfo)
	iris.Get("/admin/object", myJwtMiddleware.Serve, getObjectInfo)
	iris.Get("/admin/cachehit", myJwtMiddleware.Serve, getCacheHitRate)
	go iris.Listen(config.Address)
}

func stopAdminServer() {
	// TODO should shutdown admin API server gracefully
}
