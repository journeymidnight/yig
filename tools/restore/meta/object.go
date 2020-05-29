package meta

import (
	. "github.com/journeymidnight/yig-restore/error"
	"github.com/journeymidnight/yig-restore/helper"
	. "github.com/journeymidnight/yig-restore/meta/types"
)

func (m *Meta) GetObject(bucketName string, objectName string) (object *Object, err error) {
	getObject := func() (o interface{}, err error) {
		object, err := m.Client.GetObject(bucketName, objectName, "")
		if err != nil {
			return
		}
		helper.Logger.Info("GetObject object.Name:", object.Name)
		if object.Name != objectName {
			err = ErrNoSuchKey
			return
		}
		return object, nil
	}

	o, err := getObject()
	if err != nil {
		return
	}
	object, ok := o.(*Object)
	if !ok {
		err = ErrInternalError
		return
	}
	return object, nil
}

func (m *Meta) GetAllObject(bucketName string, objectName string) (object []*Object, err error) {
	return m.Client.GetAllObject(bucketName, objectName, "")
}

func (m *Meta) GetObjectVersion(bucketName, objectName, version string) (object *Object, err error) {
	getObjectVersion := func() (o interface{}, err error) {
		object, err := m.Client.GetObject(bucketName, objectName, version)
		if err != nil {
			return
		}
		if object.Name != objectName {
			err = ErrNoSuchKey
			return
		}
		return object, nil
	}

	o, err := getObjectVersion()
	if err != nil {
		return
	}
	object, ok := o.(*Object)
	if !ok {
		err = ErrInternalError
		return
	}
	return object, nil
}
