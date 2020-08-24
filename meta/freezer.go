package meta

import (
	. "database/sql/driver"
	"github.com/journeymidnight/yig/helper"

	"time"

	"github.com/journeymidnight/yig/meta/common"
	"github.com/journeymidnight/yig/meta/types"
)

func (m *Meta) CreateFreezer(freezer *types.Freezer) error {
	return m.Client.CreateFreezer(freezer)
}

func (m *Meta) GetFreezer(bucketName string, objectName string, version string) (freezer *types.Freezer, err error) {
	freezer, err = m.Client.GetFreezer(bucketName, objectName, version)
	if err != nil {
		return nil, err
	}
	if helper.CONFIG.FakeRestore {
		timeNow := time.Now().UTC()
		if freezer.LastModifiedTime.UnixNano() > timeNow.UnixNano() {
			freezer.Status = common.ObjectRestoring
		}
	}
	return
}

func (m *Meta) GetFreezerStatus(bucketName string, objectName string, version string) (freezer *types.Freezer, err error) {
	freezer, err = m.Client.GetFreezerStatus(bucketName, objectName, version)
	if err != nil {
		return nil, err
	}
	if helper.CONFIG.FakeRestore {
		timeNow := time.Now()
		if freezer.LastModifiedTime.UnixNano() > timeNow.UnixNano() {
			freezer.Status = common.ObjectRestoring
		}
	}
	return
}

func (m *Meta) UpdateFreezerDate(freezer *types.Freezer) error {
	return m.Client.UpdateFreezerDate(freezer.BucketName, freezer.Name, freezer.VersionId, freezer.LifeTime)
}

func (m *Meta) DeleteFreezer(freezer *types.Freezer, fakeRestore bool) (err error) {
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

	if !fakeRestore {
		err = m.PutFreezerToGarbageCollection(freezer)
		if err != nil {
			return err
		}
	}

	return err
}

func (m *Meta) PutFreezer(freezer *types.Freezer, status common.RestoreStatus) error {
	tx, err := m.Client.NewTrans()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			m.Client.AbortTrans(tx)
		}
	}()

	err = m.Client.PutFreezer(freezer, status, tx)
	if err != nil {
		return err
	}

	return m.Client.CommitTrans(tx)
}

func (m *Meta) DeleteFreezerWithoutCephObject(bucketName, objectName, version string, freezerType types.ObjectType, createTime uint64) (err error) {
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

	err = m.Client.DeleteFreezer(bucketName, objectName, version, freezerType, createTime, tx)
	if err != nil {
		return err
	}

	return
}
