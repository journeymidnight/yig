package main

import (
	"github.com/bsm/redislock"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/common"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
	"github.com/robfig/cron/v3"
	"math/rand"

	"syscall"
	"time"
)

const (
	EliminateSpec = "@daily"
)

var (
	Crontab   *cron.Cron
	yig       *storage.YigStorage
	RedisConn redis.Redis
	Locker    *redislock.Client
)

func InitializeRedis() {
	switch helper.CONFIG.RedisStore {
	case "single":
		helper.Logger.Info("Redis Mode Single, ADDR is:", helper.CONFIG.RedisAddress)
		r := redis.InitializeSingle()
		RedisConn = r.(redis.Redis)
	case "cluster":
		helper.Logger.Info("Redis Mode Cluster, ADDRs is:", helper.CONFIG.RedisGroup)
		r := redis.InitializeCluster()
		RedisConn = r.(redis.Redis)
	}
	Locker = redislock.New(redis.RedisClient)
}

func Restore(instance *storage.YigStorage) {
	InitializeRedis()
	rand.Seed(time.Now().UnixNano())
	yig = instance
	mutexs = make(map[string]*redislock.Lock)

	go autoRefreshLock()
	helper.Logger.Info("Start yig-restore success ..")
	Crontab = cron.New()
	Crontab.AddFunc(EliminateSpec, EliminateObjects)
	Crontab.Start()
}

func EliminateObjects() {
	helper.Logger.Info("Start elimination with object")
	freezers, err := yig.MetaStorage.Client.ListFreezersWithStatus(-1, common.ObjectHasRestored)
	if err != nil && err != ErrNoSuchKey {
		helper.Logger.Error("List freezer failed, err is:", err)
		return
	}
	for _, freezer := range freezers {
		go EliminateObject(freezer)
	}
}

func EliminateObject(freezer meta.Freezer) {
	if isNeedEliminate(freezer.LifeTime, freezer.LastModifiedTime) {
		for {
			var err error
			WG.Add(1)
			mutex, err := Locker.Obtain(redis.GenMutexKeyForRestore(&freezer), time.Duration(helper.CONFIG.LockTime)*time.Minute, nil)
			if err == redislock.ErrNotObtained {
				helper.Logger.Error("Lock object failed:", freezer.BucketName, freezer.Name, freezer.VersionId)
				goto out
			} else if err != nil {
				helper.Logger.Error("Lock seems does not work, so quit", err.Error())
				goto quit
			}

			//add lock to mutexs map
			mux.Lock()
			mutexs[mutex.Key()] = mutex
			mux.Unlock()

			err = yig.EliminateObject(&freezer)
			if err != nil {
				helper.Logger.Error("Eliminate object err:", err)
				goto release
			}
			helper.Logger.Info("EliminateObject finished", freezer)
		release:
			mutex.Release()
			mux.Lock()
			delete(mutexs, mutex.Key())
			mux.Unlock()
			WG.Done()
			break
		out:
			WG.Done()
			break
		quit:
			SignalQueue <- syscall.SIGQUIT
		}
	}
}

func isNeedEliminate(lifeTime int, lastModifiedTime time.Time) bool {
	timeNow := time.Now().Unix()
	deadLine := lastModifiedTime.AddDate(0, 0, lifeTime).Unix()
	if deadLine <= timeNow {
		return true
	}
	return false
}
