package storage

import (
	. "github.com/journeymidnight/yig-restore/error"
	. "github.com/journeymidnight/yig-restore/meta/types"
)

func (yig *Storage) EliminateObject(freezer *Freezer) (err error) {
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

func (yig *Storage) removeByFreezer(freezer *Freezer) (err error) {
	err = yig.MetaStorage.DeleteFreezer(freezer)
	if err != nil {
		return
	}
	return nil
}
