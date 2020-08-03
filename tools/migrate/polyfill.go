package main

import (
	"fmt"
	"github.com/journeymidnight/yig/backend"
	"github.com/journeymidnight/yig/meta/types"
	redis2 "github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
	"math/rand"
	"sync"
	"time"
)

// FIXME too many errors ignored (also in Yig)
func (w MigrateWorker) InvalidCache(object types.Object) {
	key := fmt.Sprintf("%s:%s:%s",
		object.BucketName, object.Name, object.VersionId)
	hashKey, _ := redis2.HashSum(key)
	_, _ = w.redisClient.Del(redis2.ObjectTable.String() + hashKey).Result()
	_, _ = w.redisClient.Del(redis2.FileTable.String() + hashKey).Result()
}

/*
	moved and modified from storage/object.go
*/
var cMap sync.Map

// 0 is for SMALL_FILE_POOLNAME, 1 is for BIG_FILE_POOLNAME, 2 is for GLACIER_FILE_POOLNAME
var latestQueryTime [3]time.Time

func pickRandomCluster(clusters map[string]backend.Cluster) (cluster backend.Cluster) {
	for _, c := range clusters {
		cluster = c
		break
	}
	return
}

func (w MigrateWorker) pickSpecificCluster(poolName string) (cluster backend.Cluster) {

	var idx int
	if poolName == backend.BIG_FILE_POOLNAME {
		idx = 1
	} else {
		idx = 0
	}

	if v, ok := cMap.Load(poolName); ok {
		return v.(backend.Cluster)
	}

	// TODO: Add Ticker to change Map
	var needCheck bool
	queryTime := latestQueryTime[idx]
	if time.Since(queryTime).Hours() > 24 { // check used space every 24 hours
		latestQueryTime[idx] = time.Now()
		needCheck = true
	}
	var totalWeight int
	clusterWeights := make(map[string]int, len(w.cephClusters))
	metaClusters, err := w.tikvClient.GetClusters()
	if err != nil {
		cluster = pickRandomCluster(w.cephClusters)
		return
	}
	for _, cluster := range metaClusters {
		if cluster.Weight == 0 {
			continue
		}
		if cluster.Pool != poolName {
			continue
		}
		if needCheck {
			c, ok := w.cephClusters[cluster.Fsid]
			if !ok {
				w.logger.Warn("Cluster not configured:", cluster.Fsid)
			}
			usage, err := c.GetUsage()
			if err != nil {
				w.logger.Warn("Error getting used space:", err,
					"fsid:", cluster.Fsid)
				continue
			}
			if usage.UsedSpacePercent > storage.CLUSTER_MAX_USED_SPACE_PERCENT {
				w.logger.Warn("Cluster used space exceed ",
					storage.CLUSTER_MAX_USED_SPACE_PERCENT, cluster.Fsid)
				continue
			}
		}
		totalWeight += cluster.Weight
		clusterWeights[cluster.Fsid] = cluster.Weight
	}
	if len(clusterWeights) == 0 || totalWeight == 0 {
		cluster = pickRandomCluster(w.cephClusters)
		return
	}
	N := rand.Intn(totalWeight)
	n := 0
	for fsid, weight := range clusterWeights {
		n += weight
		if n > N {
			cluster = w.cephClusters[fsid]
			break
		}
	}

	cMap.Store(poolName, cluster)
	return
}
