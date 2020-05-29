package meta

import (
	. "database/sql/driver"

	"github.com/journeymidnight/yig-restore/meta/common"
	"github.com/journeymidnight/yig-restore/meta/types"
)

func (m *Meta) GetFreezer(bucketName string, objectName string, version string) (object *types.Freezer, err error) {
	return m.Client.GetFreezer(bucketName, objectName, version)
}

func (m *Meta) PutFreezer(freezer *types.Freezer, status common.Status) error {
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

	return
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
