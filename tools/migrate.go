package main

import (
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/journeymidnight/yig/backend"
	"github.com/journeymidnight/yig/crypto"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/mods"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
)

const (
	WATER_LOW           = 120
	TASKQ_MAX_LENGTH    = 200
	SCAN_LIMIT          = 50
	DEFAULT_MG_LOG_PATH = "/var/log/yig/migrate.log"
)

var (
	yigs        []*storage.YigStorage
	signalQueue chan os.Signal
	mgWaitgroup sync.WaitGroup
	mgStop      bool
	mgTaskQ     chan types.Object
)

func checkAndDoMigrate(index int) {
	for {
		if mgStop {
			helper.Logger.Info("Shutting down...")
			return
		}
		var oid, newOid string
		var bytesWritten uint64
		var err error
		var sourceCluster, destCluster backend.Cluster
		var reader io.ReadCloser
		object := <-mgTaskQ
		mgWaitgroup.Add(1)
		if object.LastModifiedTime.Add(time.Second * time.Duration(helper.CONFIG.MgObjectCooldown)).After(time.Now()) {
			goto release
		}
		sourceCluster = yigs[index].DataStorage[object.Location]
		reader, err = sourceCluster.GetReader(object.Pool, object.ObjectId, 0, uint64(object.Size))
		if err != nil {
			helper.Logger.Info("checkIfNeedMigrate GetReader failed:", object.Pool, object.ObjectId, err.Error())
			goto quit
		}

		destCluster = yigs[index].PickSpecificCluster(backend.BIG_FILE_POOLNAME)
		newOid, bytesWritten, err = destCluster.Append(backend.BIG_FILE_POOLNAME, newOid, reader, 0)
		if err != nil {
			helper.Logger.Error("cephCluster.Append err:", err, newOid)
			goto quit
		}
		if bytesWritten != uint64(object.Size) {
			destCluster.Remove(backend.BIG_FILE_POOLNAME, newOid)
			helper.Logger.Error("cephCluster.Append write length to hdd not equel the object size:", newOid)
			goto quit
		}
		//update object fileds
		object.Location = destCluster.ID()
		object.Pool = backend.BIG_FILE_POOLNAME
		oid = object.ObjectId
		object.ObjectId = newOid
		//update objects table and remove entry from hotobjects
		err = yigs[index].MetaStorage.MigrateObject(&object)
		if err != nil {
			destCluster.Remove(backend.BIG_FILE_POOLNAME, newOid)
			helper.Logger.Error("cephCluster.Append MigrateObject failed:", err.Error())
			goto quit
		}
		//remove data from ssd cluster
		err = sourceCluster.Remove(backend.SMALL_FILE_POOLNAME, oid)
		if err != nil {
			helper.Logger.Error("cephCluster.Append Remove data from rabbit failed:", err.Error())
			goto quit
		}
		//invalid redis cache
		yigs[index].MetaStorage.Cache.Remove(redis.ObjectTable, object.BucketName+":"+object.Name+":"+object.VersionId)
		yigs[index].DataCache.Remove(object.BucketName + ":" + object.Name + ":" + object.VersionId)
		goto release
	quit:
		signalQueue <- syscall.SIGQUIT
	release:
		mgWaitgroup.Done()
	}
}

func getHotObjects() {
	var bMarker, oMarker, vMarker string
	helper.Logger.Info("getHotObjects thread start")
	var objects []types.Object
	for {
		if mgStop {
			helper.Logger.Info("shutting down...")
			return
		}
	wait:
		if len(mgTaskQ) >= WATER_LOW {
			time.Sleep(time.Duration(10) * time.Millisecond)
			goto wait
		}

		objects = objects[:0]
		helper.Logger.Info("start to scan hotobjects", bMarker, oMarker, vMarker)
		result, err := yigs[0].MetaStorage.ScanHotObjects(SCAN_LIMIT, bMarker, oMarker, vMarker)
		if err != nil {
			helper.Logger.Info("getHotObjects quit of error...", err.Error())
			signalQueue <- syscall.SIGQUIT
			return
		}

		for _, object := range result.Objects {
			mgTaskQ <- object
			bMarker = object.BucketName
			oMarker = object.Name
			vMarker = object.VersionId
		}

		if len(result.Objects) < SCAN_LIMIT {
			bMarker = ""
			oMarker = ""
			vMarker = ""
			helper.Logger.Info("scan job end success, sleep seconds:", helper.CONFIG.MgScanInterval)
			time.Sleep(time.Duration(helper.CONFIG.MgScanInterval) * time.Second)
		}
	}
}

func main() {
	mgStop = false

	helper.SetupConfig()
	logLevel := log.ParseLevel(helper.CONFIG.LogLevel)

	helper.Logger = log.NewFileLogger(DEFAULT_MG_LOG_PATH, logLevel)
	defer helper.Logger.Close()
	mgTaskQ = make(chan types.Object, TASKQ_MAX_LENGTH)
	signal.Ignore()
	signalQueue = make(chan os.Signal)
	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		redis.Initialize()
		defer redis.CloseAll()
	}
	// Read all *.so from plugins directory, and fill the variable allPlugins
	allPluginMap := mods.InitialPlugins()
	kms := crypto.NewKMS(allPluginMap)

	numOfWorkers := helper.CONFIG.MgThread
	yigs = make([]*storage.YigStorage, helper.CONFIG.MgThread+1)
	yigs[0] = storage.New(int(meta.NoCache), false, kms)
	helper.Logger.Info("start migrate thread:", numOfWorkers)
	for i := 0; i < numOfWorkers; i++ {
		yigs[i+1] = storage.New(helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, kms)
		if helper.CONFIG.CacheCircuitCheckInterval != 0 && helper.CONFIG.MetaCacheType != 0 {
			for j := 0; j < len(helper.CONFIG.RedisGroup); j++ {
				go func(j int) {
					yigs[i+1].PingCache(time.Duration(helper.CONFIG.CacheCircuitCheckInterval)*time.Second, j)
				}(j)
			}
		}
		go checkAndDoMigrate(i + 1)
	}
	go getHotObjects()
	signal.Notify(signalQueue, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGHUP)
	for {
		s := <-signalQueue
		switch s {
		case syscall.SIGHUP:
			// reload config file
			helper.SetupConfig()
		default:
			// coolStop YIG server, order matters
			mgStop = true
			mgWaitgroup.Wait()
			return
		}
	}

}
