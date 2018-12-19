package main

import (
	"encoding/json"
	"github.com/dgrijalva/jwt-go"
	router "github.com/gorilla/mux"
	"github.com/journeymidnight/yig/api"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/log"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

type adminServerConfig struct {
	Address string
	Logger  *log.Logger
	Yig     *storage.YigStorage
}

type userJson struct {
	Buckets []string
	Keys    []common.Credential
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
		api.WriteErrorResponse(w, r, err)
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
		api.WriteErrorResponse(w, r, err)
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
		api.WriteErrorResponse(w, r, err)
		return
	}
	helper.Debugln("enter getUserInfo", uid, buckets)

	var keys []common.Credential
	if helper.CONFIG.DebugMode == false {
		keys, err = iam.GetKeysByUid(uid)
		if err != nil {
			api.WriteErrorResponse(w, r, err)
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

	object, err := adminServer.Yig.MetaStorage.GetObject(bucketName, objectName, true)
	if err != nil {
		api.WriteErrorResponse(w, r, err)
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
	//	SetJwtMiddlewareHandler,
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
	admin.Methods("GET").Path("/usage").HandlerFunc(SetJwtMiddlewareFunc(getUsage))
	admin.Methods("GET").Path("/user").HandlerFunc(SetJwtMiddlewareFunc(getUserInfo))
	admin.Methods("GET").Path("/bucket").HandlerFunc(SetJwtMiddlewareFunc(getBucketInfo))
	admin.Methods("GET").Path("/object").HandlerFunc(SetJwtMiddlewareFunc(getObjectInfo))
	admin.Methods("GET").Path("/cachehit").HandlerFunc(SetJwtMiddlewareFunc(getCacheHitRatio))

	metrics := NewMetrics("yig")
	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics)

	apiRouter.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	apiRouter.Path("/debug/cmdline").HandlerFunc(pprof.Cmdline)
	apiRouter.Path("/debug/profile").HandlerFunc(pprof.Profile)
	apiRouter.Path("/debug/symbol").HandlerFunc(pprof.Symbol)
	apiRouter.Path("/debug/trace").HandlerFunc(pprof.Trace)
	apiRouter.PathPrefix("/debug/pprof/").HandlerFunc(pprof.Index)

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

	logger.Println(5, "\nS3 Object Storage:")
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
