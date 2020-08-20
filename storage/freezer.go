package storage

import (
	"io"
	"math/rand"
	"time"

	"github.com/journeymidnight/yig/backend"

	"github.com/journeymidnight/yig/meta/common"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	meta "github.com/journeymidnight/yig/meta/types"
)

const (
	SIZE_SUM_MIN = 64 << 20 // 64M
	SIZE_SUM_MAX = 4 << 30  // 4G
)

func (yig *YigStorage) GetFreezerStatus(bucketName string, objectName string, version string) (freezer *meta.Freezer, err error) {
	return yig.MetaStorage.GetFreezerStatus(bucketName, objectName, version)
}

func (yig *YigStorage) CreateFreezer(freezer *meta.Freezer) (err error) {
	return yig.MetaStorage.CreateFreezer(freezer)
}

func (yig *YigStorage) GetFreezer(bucketName string, objectName string, version string) (freezer *meta.Freezer, err error) {
	return yig.MetaStorage.GetFreezer(bucketName, objectName, version)
}

func (yig *YigStorage) UpdateFreezerDate(freezer *meta.Freezer, date int, isIncrement bool) (err error) {
	if date > 30 || date < 1 {
		return ErrInvalidRestoreDate
	}
	var lifeTime int
	if isIncrement {
		freezerInfo, err := yig.GetFreezer(freezer.BucketName, freezer.Name, freezer.VersionId)
		if err != nil {
			return err
		}
		lifeTime = freezerInfo.LifeTime + date
		if lifeTime > 30 {
			return ErrInvalidRestoreDate
		}
	} else {
		lifeTime = date
	}
	freezer.LifeTime = lifeTime
	return yig.MetaStorage.UpdateFreezerDate(freezer)
}

func (yig *YigStorage) EliminateObject(freezer *meta.Freezer) (err error) {
	obj, err := yig.MetaStorage.GetFreezer(freezer.BucketName, freezer.Name, freezer.VersionId)
	if err == ErrNoSuchKey {
		return nil
	}
	if err != nil {
		return err
	}
	err = yig.removeByFreezer(obj)
	if err != nil {
		return err
	}
	return
}

func (yig *YigStorage) removeByFreezer(freezer *meta.Freezer) (err error) {
	return yig.MetaStorage.DeleteFreezer(freezer, helper.CONFIG.FakeRestore)
}

func (yig *YigStorage) pickCluster() (cluster backend.Cluster, poolName string) {

	var idx int
	poolName = backend.BIG_FILE_POOLNAME
	idx = 1

	if v, ok := cMap.Load(poolName); ok {
		return v.(backend.Cluster), poolName
	}

	// TODO: Add Ticker to change Map
	var needCheck bool
	queryTime := latestQueryTime[idx]
	if time.Since(queryTime).Hours() > 24 { // check used space every 24 hours
		latestQueryTime[idx] = time.Now()
		needCheck = true
	}
	var totalWeight int
	clusterWeights := make(map[string]int, len(yig.DataStorage))
	metaClusters, err := yig.MetaStorage.GetClusters()
	if err != nil {
		cluster = yig.pickRandomCluster()
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
			usage, err := yig.DataStorage[cluster.Fsid].GetUsage()
			if err != nil {
				helper.Logger.Warn("Error getting used space: ", err,
					"fsid: ", cluster.Fsid)
				continue
			}
			if usage.UsedSpacePercent > CLUSTER_MAX_USED_SPACE_PERCENT {
				helper.Logger.Warn("Cluster used space exceed ",
					CLUSTER_MAX_USED_SPACE_PERCENT, cluster.Fsid)
				continue
			}
		}
		totalWeight += cluster.Weight
		clusterWeights[cluster.Fsid] = cluster.Weight
	}
	if len(clusterWeights) == 0 || totalWeight == 0 {
		cluster = yig.pickRandomCluster()
		return
	}
	N := rand.Intn(totalWeight)
	n := 0
	for fsid, weight := range clusterWeights {
		n += weight
		if n > N {
			cluster = yig.DataStorage[fsid]
			break
		}
	}
	cMap.Store(poolName, cluster)
	return
}

func (yig *YigStorage) RestoreObject(targetObject *meta.Freezer, source io.Reader, needUpdateStatus bool) (err error) {
	var oid string
	var maybeObjectToRecycle objectToRecycle

	if helper.CONFIG.FakeRestore {
		if needUpdateStatus {
			err = yig.MetaStorage.Client.UpdateFreezerStatus(targetObject.BucketName, targetObject.Name, targetObject.VersionId, common.ObjectNeedRestore, common.ObjectRestoring)
			if err != nil {
				helper.Logger.Error("Upload Freezer status failed!")
				return
			}
		}

		// Limit the reader to its provided size if specified.
		var limitedDataReader io.Reader
		limitedDataReader = io.LimitReader(source, targetObject.Size)

		cephCluster, poolName := yig.pickCluster()

		if len(targetObject.Parts) != 0 {
			var targetParts = make(map[int]*meta.Part, len(targetObject.Parts))
			for i := 1; i <= len(targetObject.Parts); i++ {
				part := targetObject.Parts[i]
				targetParts[i] = part
				err = func() (err error) {
					pr, pw := io.Pipe()
					defer pr.Close()
					var total = part.Size
					go func() {
						_, err = io.CopyN(pw, source, total)
						if err != nil {
							return
						}
						pw.Close()
					}()
					var bytesW uint64
					oid, bytesW, err = cephCluster.Put(poolName, pr)
					maybeObjectToRecycle = objectToRecycle{
						location: cephCluster.ID(),
						pool:     poolName,
						objectId: oid,
					}
					if bytesW < uint64(part.Size) {
						RecycleQueue <- maybeObjectToRecycle
						helper.Logger.Error("Copy part", i, "error:", bytesW, part.Size)
						return ErrIncompleteBody
					}
					if err != nil {
						return err
					}
					part.LastModified = time.Now().UTC().Format(meta.CREATE_TIME_LAYOUT)
					part.ObjectId = oid
					return nil
				}()
				if err != nil {
					return err
				}
			}
			targetObject.ObjectId = ""
			targetObject.Parts = targetParts
		} else {
			var bytesWritten uint64
			oid, bytesWritten, err = cephCluster.Put(poolName, limitedDataReader)
			if err != nil {
				return
			}
			// Should metadata update failed, add `maybeObjectToRecycle` to `RecycleQueue`,
			// so the object in Ceph could be removed asynchronously
			maybeObjectToRecycle = objectToRecycle{
				location: cephCluster.ID(),
				pool:     poolName,
				objectId: oid,
			}
			if int64(bytesWritten) < targetObject.Size {
				RecycleQueue <- maybeObjectToRecycle
				helper.Logger.Warn("Copy ", "error:", bytesWritten, targetObject.Size)
				return ErrIncompleteBody
			}
			targetObject.ObjectId = oid
		}
		// TODO validate bucket policy and fancy ACL

		targetObject.Location = cephCluster.ID()
		targetObject.Pool = poolName
	} else {
		var timeNum int64
		if targetObject.Size < SIZE_SUM_MIN {
			timeNum = rand.Int63n(10)
		} else if targetObject.Size < SIZE_SUM_MAX && targetObject.Size > SIZE_SUM_MIN {
			timeNum = targetObject.Size/SIZE_SUM_MIN + rand.Int63n(10) + 10
		} else {
			timeNum = targetObject.Size/SIZE_SUM_MAX + rand.Int63n(10) + 60
		}
		time.Sleep(time.Duration(timeNum))
	}

	targetObject.LastModifiedTime = time.Now().UTC()
	targetObject.Status, err = common.MatchStatusIndex("RESTORING")
	if err != nil {
		helper.Logger.Error("Update status err:", err)
		return err
	}

	err = yig.MetaStorage.PutFreezer(targetObject, common.ObjectHasRestored)
	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	}

	return nil
}
