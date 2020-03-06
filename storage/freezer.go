package storage

import (
	. "github.com/journeymidnight/yig/error"
	meta "github.com/journeymidnight/yig/meta/types"
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
		return ErrInvalidRestoreInfo
	}
	var lifeTime int
	if isIncrement {
		lifeTime = freezer.LifeTime + date
		if lifeTime > 30 {
			lifeTime = 30
		}
	} else {
		lifeTime = date
	}
	freezer.LifeTime = lifeTime
	return yig.MetaStorage.UpdateFreezerDate(freezer)
}
