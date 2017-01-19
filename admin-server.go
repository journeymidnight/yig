package main

import (
	"log"
	"net/http"
	"legitlab.letv.cn/yig/yig/helper"
	"legitlab.letv.cn/yig/yig/iam"
	"legitlab.letv.cn/yig/yig/storage"
	router "github.com/gorilla/mux"
	"github.com/dgrijalva/jwt-go"
	"net"
	"time"
	"legitlab.letv.cn/yig/yig/api"
	"encoding/json"
	"legitlab.letv.cn/yig/yig/meta"
)

type adminServerConfig struct {
	Address string
	Logger  *log.Logger
	Yig     *storage.YigStorage
}

type userJson struct {
	Buckets []string
	Keys    []iam.AccessKeyItem
}

type bucketJson struct {
	Bucket meta.Bucket
}

type objectJson struct {
	Object *meta.Object
}

type cacheJson struct {
	HitRate float64
}

type usageJson struct {
	Usage int64
}

var adminServer *adminServerConfig
type handlerFunc func(http.Handler) http.Handler


func getUsage(w http.ResponseWriter, r *http.Request) {
	claims := r.Context().Value("claims").(jwt.MapClaims)
	bucketName := claims["bucket"].(string)

	usage, err := adminServer.Yig.MetaStorage.GetUsage(bucketName)
	if err != nil {
		api.WriteErrorResponse(w,r,err)
		return
	}
	b, err := json.Marshal(usageJson{Usage: usage})
	w.Write(b)
	return
}

func getBucketInfo(w http.ResponseWriter, r *http.Request) {
	claims := r.Context().Value("claims").(jwt.MapClaims)
	bucketName := claims["bucket"].(string)

	helper.Debugln("bucketName:", bucketName)
	bucket, err := adminServer.Yig.MetaStorage.GetBucketInfo(bucketName)
	if err != nil {
		api.WriteErrorResponse(w,r,err)
		return
	}

	b, err := json.Marshal(bucketJson{Bucket: bucket})
	w.Write(b)
	return
}

func getUserInfo(w http.ResponseWriter, r *http.Request) {
	claims := r.Context().Value("claims").(jwt.MapClaims)
	uid := claims["uid"].(string)

	buckets, err := adminServer.Yig.MetaStorage.GetUserInfo(uid)
	if err != nil {
		api.WriteErrorResponse(w,r,err)
		return
	}
	helper.Debugln("enter getUserInfo", uid, buckets)

	var keys []iam.AccessKeyItem
	if helper.CONFIG.DebugMode == false {
		keys, err = iam.GetKeysByUid(uid)
		if err != nil {
			api.WriteErrorResponse(w,r,err)
			return
		}
	}
	b, err := json.Marshal(userJson{Buckets: buckets, Keys: keys})
	w.Write(b)
	return
}

func getObjectInfo(w http.ResponseWriter, r *http.Request) {
	helper.Debugln("enter getObjectInfo")
	claims := r.Context().Value("claims").(jwt.MapClaims)
	bucketName := claims["bucket"].(string)
	objectName := claims["object"].(string)

	object, err := adminServer.Yig.MetaStorage.GetObject(bucketName, objectName)
	if err != nil {
		api.WriteErrorResponse(w,r,err)
		return
	}
	b, err := json.Marshal(objectJson{Object: object})
	w.Write(b)
	return
}

func getCacheHitRatio(w http.ResponseWriter, r *http.Request) {
	helper.Debugln("enter getCacheHitRatio")

	rate := adminServer.Yig.MetaStorage.Cache.GetCacheHitRatio()
	b, _ := json.Marshal(cacheJson{HitRate: rate})
	w.Write(b)
	return
}

var handlerFns = []handlerFunc{
	SetJwtMiddlewareHandler,
}

func RegisterHandlers(router *router.Router, handlerFns ...handlerFunc) http.Handler {
	var f http.Handler
	f = router
	for _, hFn := range handlerFns {
		f = hFn(f)
	}
	return f
}

func configureAdminHandler() http.Handler {
	mux := router.NewRouter()
	apiRouter := mux.NewRoute().PathPrefix("/").Subrouter()
	admin := apiRouter.PathPrefix("/admin").Subrouter()
	admin.Methods("GET").Path("/usage").HandlerFunc(getUsage)
	admin.Methods("GET").Path("/user").HandlerFunc(getUserInfo)
	admin.Methods("GET").Path("/bucket").HandlerFunc(getBucketInfo)
	admin.Methods("GET").Path("/object").HandlerFunc(getObjectInfo)
	admin.Methods("GET").Path("/cachehit").HandlerFunc(getCacheHitRatio)

	handle := RegisterHandlers(mux, handlerFns...)
	return handle
}

func startAdminServer(c *adminServerConfig) {
	adminServer = c
	serverAddress := c.Address
	host, port, _ := net.SplitHostPort(serverAddress)
	// If port empty, default to port '80'
	if port == "" {
		port = "9000"
	}

	// Check if requested port is available.
	checkPortAvailability(getPort(net.JoinHostPort(host, port)))

	adminServer := &http.Server{
		Addr: c.Address,
		// Adding timeout of 10 minutes for unresponsive client connections.
		ReadTimeout:    10 * time.Minute,
		WriteTimeout:   10 * time.Minute,
		Handler:        configureAdminHandler(),
		MaxHeaderBytes: 1 << 20,
	}


	hosts, port := getListenIPs(adminServer) // get listen ips and port.

	logger.Println("\nS3 Object Storage:")
	// Print api listen ips.
	printListenIPs(false, hosts, port)

	go func() {
		var err error
		// Configure TLS if certs are available.
		err = adminServer.ListenAndServe()
		helper.FatalIf(err, "API server error.")
	}()
}

func stopAdminServer() {
	// TODO should shutdown admin API server gracefully
}
