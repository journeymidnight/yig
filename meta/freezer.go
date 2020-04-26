package meta

import (
	. "database/sql/driver"

	"github.com/journeymidnight/yig/meta/types"
)

func (m *Meta) CreateFreezer(freezer *types.Freezer) error {
	return m.Client.CreateFreezer(freezer)
}

func (m *Meta) GetFreezer(bucketName string, objectName string, version string) (freezer *types.Freezer, err error) {
	return m.Client.GetFreezer(bucketName, objectName, version)
}

func (m *Meta) GetFreezerStatus(bucketName string, objectName string, version string) (freezer *types.Freezer, err error) {
	return m.Client.GetFreezerStatus(bucketName, objectName, version)
}

func (m *Meta) UpdateFreezerDate(freezer *types.Freezer) error {
	return m.Client.UploadFreezerDate(freezer.BucketName, freezer.Name, freezer.VersionId, freezer.LifeTime)
}

func (m *Meta) DeleteFreezer(freezer *types.Freezer) (err error) {
	var tx Tx
	tx, err = m.Client.NewTrans()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = m.Client.CommitTrans(tx)
		}
		if err != nil {
			m.Client.AbortTrans(tx)
		}
	}()

	err = m.Client.DeleteFreezer(freezer.BucketName, freezer.Name, freezer.VersionId, freezer.Type, freezer.CreateTime, tx)
	if err != nil {
		return err
	}

	err = m.Client.PutFreezerToGarbageCollection(freezer, tx)
	if err != nil {
		return err
	}

	return err
}
