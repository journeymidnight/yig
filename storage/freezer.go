package storage

import (
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
