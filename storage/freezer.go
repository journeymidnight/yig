package storage

import (
	"math/rand"
	"time"

	"github.com/journeymidnight/yig/meta/common"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	meta "github.com/journeymidnight/yig/meta/types"
)

const (
	SUM_BLOCKSIZE              = 100 << 20 // 100M
	SUM_BENCHMARKTIMECONSUMING = 1         // 1s
	SUM_BASETIME               = 30        // 30s
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
	return yig.MetaStorage.DeleteFreezer(freezer)
}

func (yig *YigStorage) RestoreObject(freezer *meta.Freezer) (err error) {
	err = yig.MetaStorage.CreateFreezer(freezer)
	if err != nil {
		helper.Logger.Error("CreateFreezer err:", err)
		return err
	}

	var timeNum int64
	// Fake thawing time simulation, calculate the thawing time according to the size and add the simulated jitter delay and the reference time
	timeNum = (freezer.Size/SUM_BLOCKSIZE)*SUM_BENCHMARKTIMECONSUMING + rand.Int63n(60) + SUM_BASETIME
	if timeNum > 300 {
		timeNum = 300
	}
	timeNow := time.Now().UTC()
	freezer.LastModifiedTime = timeNow.Add(time.Duration(timeNum) * time.Second)
	freezer.Status, err = common.MatchStatusIndex("RESTORING")
	if err != nil {
		helper.Logger.Error("Update status err:", err)
		return err
	}

	err = yig.MetaStorage.PutFreezer(freezer, common.ObjectHasRestored)
	if err != nil {
		return err
	}
	return nil
}
