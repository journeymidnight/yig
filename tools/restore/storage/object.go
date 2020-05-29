package storage

import (
	"errors"
	"github.com/journeymidnight/yig-restore/backend"
	. "github.com/journeymidnight/yig-restore/error"
	"github.com/journeymidnight/yig-restore/helper"
	"github.com/journeymidnight/yig-restore/meta/common"
	meta "github.com/journeymidnight/yig-restore/meta/types"
	"io"
	"math/rand"
	"sync"
	"time"
)

var cMap sync.Map
var latestQueryTime [3]time.Time // 0 is for SMALL_FILE_POOLNAME, 1 is for BIG_FILE_POOLNAME, 2 is for GLACIER_FILE_POOLNAME

const (
	CLUSTER_MAX_USED_SPACE_PERCENT = 85
)

func (yig *Storage) GetObjectInfo(bucketName string, objectName string,
	version string) (object *meta.Object, err error) {

	if version == "" {
		object, err = yig.MetaStorage.GetObject(bucketName, objectName)
	} else {
		object, err = yig.getObjWithVersion(bucketName, objectName, version)
	}
	if err != nil {
		return
	}
	return
}

func (yig *Storage) getObjWithVersion(bucketName, objectName, version string) (object *meta.Object, err error) {
	return yig.MetaStorage.GetObjectVersion(bucketName, objectName, version)
}

func (yig *Storage) GetObject(object *meta.Object, startOffset int64,
	length int64, writer io.Writer) (err error) {
	if len(object.Parts) == 0 { // this object has only one part
		cephCluster, ok := yig.DataStorage[object.Location]
		if !ok {
			return errors.New("Cannot find specified ceph cluster: " + object.Location)
		}
		transWholeObjectWriter := generateTransWholeObjectFunc(cephCluster, object)
		return transWholeObjectWriter(writer)
	}

	// multipart uploaded object
	var low = object.PartsIndex.SearchLowerBound(startOffset)
	if low == -1 {
		low = 1
	} else {
		//parts number starts from 1, so plus 1 here
		low += 1
	}

	for i := low; i <= len(object.Parts); i++ {
		p := object.Parts[i]
		//for high
		if p.Offset > startOffset+length {
			return
		}
		//for low
		{
			var readOffset, readLength int64
			if startOffset <= p.Offset {
				readOffset = 0
			} else {
				readOffset = startOffset - p.Offset
			}
			if p.Offset+p.Size <= startOffset+length {
				readLength = p.Size - readOffset
			} else {
				readLength = startOffset + length - (p.Offset + readOffset)
			}
			cluster, ok := yig.DataStorage[object.Location]
			if !ok {
				return errors.New("Cannot find specified ceph cluster: " +
					object.Location)
			}
			transPartFunc := generateTransPartObjectFunc(cluster, object, p, readOffset, readLength)
			err := transPartFunc(writer)
			if err != nil {
				return nil
			}
			continue
		}
	}
	return
}

func (yig *Storage) RestoreObject(targetObject *meta.Freezer, source io.Reader, needUpdateStatus bool) (err error) {
	var oid string
	var maybeObjectToRecycle objectToRecycle

	if needUpdateStatus {
		err = yig.MetaStorage.Client.UploadFreezerStatus(targetObject.BucketName, targetObject.Name, targetObject.VersionId, common.Status(0), common.Status(1))
		if err != nil {
			helper.Logger.Error("Upload Freezer status failed!")
			return
		}
	}

	// Limit the reader to its provided size if specified.
	var limitedDataReader io.Reader
	limitedDataReader = io.LimitReader(source, targetObject.Size)

	cephCluster, poolName := yig.pickClusterAndPool(targetObject.BucketName,
		targetObject.Name, targetObject.Size, false)

	if len(targetObject.Parts) != 0 {
		var targetParts = make(map[int]*meta.Part, len(targetObject.Parts))
		//		etaglist := make([]string, len(sourceObject.Parts))
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
			helper.Logger.Error("Copy ", "error:", bytesWritten, targetObject.Size)
			return ErrIncompleteBody
		}
		targetObject.ObjectId = oid
	}
	// TODO validate bucket policy and fancy ACL

	targetObject.Location = cephCluster.ID()
	targetObject.Pool = poolName
	targetObject.LastModifiedTime = time.Now().UTC()
	targetObject.Status, err = common.MatchStatusIndex("RESTORING")
	if err != nil {
		helper.Logger.Error("Update status err:", err)
		return err
	}

	err = yig.MetaStorage.PutFreezer(targetObject, common.Status(2))
	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	}

	return nil
}

/*this pool is for download only */
var (
	downloadBufPool sync.Pool
)

func init() {
	downloadBufPool.New = func() interface{} {
		return make([]byte, helper.Conf.DownloadBufPoolSize)
	}
}

func generateTransWholeObjectFunc(cluster backend.Cluster,
	object *meta.Object) func(io.Writer) error {

	getWholeObject := func(w io.Writer) error {
		reader, err := cluster.GetReader(object.Pool, object.ObjectId,
			0, uint64(object.Size))
		if err != nil {
			return nil
		}
		defer reader.Close()

		buf := downloadBufPool.Get().([]byte)
		_, err = io.CopyBuffer(w, reader, buf)
		downloadBufPool.Put(buf)
		return err
	}
	return getWholeObject
}

func generateTransPartObjectFunc(cephCluster backend.Cluster, object *meta.Object, part *meta.Part, offset, length int64) func(io.Writer) error {
	getNormalObject := func(w io.Writer) error {
		var oid string
		/* the transfered part could be Part or Object */
		if part != nil {
			oid = part.ObjectId
		} else {
			oid = object.ObjectId
		}
		reader, err := cephCluster.GetReader(object.Pool, oid, offset, uint64(length))
		if err != nil {
			return nil
		}
		defer reader.Close()
		buf := downloadBufPool.Get().([]byte)
		_, err = io.CopyBuffer(w, reader, buf)
		downloadBufPool.Put(buf)
		return err
	}
	return getNormalObject
}

func (yig *Storage) pickClusterAndPool(bucket string, object string,
	size int64, isAppend bool) (cluster backend.Cluster, poolName string) {

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

func (yig *Storage) GetClusterByFsName(fsName string) (cluster backend.Cluster, err error) {
	if c, ok := yig.DataStorage[fsName]; ok {
		cluster = c
	} else {
		err = errors.New("Cannot find specified ceph cluster: " + fsName)
	}
	return
}

func (yig *Storage) pickRandomCluster() (cluster backend.Cluster) {
	helper.Logger.Warn("Error picking cluster from table cluster in DB, " +
		"use first cluster in config to write.")
	for _, c := range yig.DataStorage {
		cluster = c
		break
	}
	return
}
