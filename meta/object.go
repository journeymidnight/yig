package meta

import (
	. "database/sql/driver"
	"time"

	"github.com/journeymidnight/yig/meta/common"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/backend"
	. "github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
)

func (m *Meta) GetObject(bucketName, objectName, reqVersion string, willNeed bool) (object *Object, err error) {
	if reqVersion == "null" {
		reqVersion = NullVersion
	}
	getObjectVersion := func() (o interface{}, err error) {
		if reqVersion == "" {
			object, err = m.Client.GetLatestObjectVersion(bucketName, objectName)
			if err != nil {
				return
			}
		} else {
			object, err = m.Client.GetObject(bucketName, objectName, reqVersion, nil)
			if err != nil {
				return
			}
		}
		return object, nil
	}
	unmarshaller := func(in []byte) (interface{}, error) {
		var object Object
		err := helper.MsgPackUnMarshal(in, &object)
		return &object, err
	}

	var o interface{}
	if reqVersion != "" {
		o, err = m.Cache.Get(redis.ObjectTable, bucketName+":"+objectName+":"+reqVersion,
			getObjectVersion, unmarshaller, willNeed)
		if err != nil {
			return
		}
	} else {
		o, err = getObjectVersion()
		if err != nil {
			return
		}
	}
	object, ok := o.(*Object)
	if !ok {
		err = ErrInternalError
		return
	}
	return object, nil
}

func (m *Meta) PutObject(reqCtx RequestContext, object *Object, multipart *Multipart, updateUsage bool) (deltaInfo map[common.StorageClass]int64, err error) {
	deltaInfo = make(map[common.StorageClass]int64)
	if reqCtx.BucketInfo == nil {
		return deltaInfo, ErrNoSuchBucket
	}
	switch reqCtx.BucketInfo.Versioning {
	case datatype.BucketVersioningSuspended:
		// TODO: Check SUSPEND Logic
		fallthrough
	case datatype.BucketVersioningDisabled:
		needUpdate := (reqCtx.ObjectInfo != nil)
		if needUpdate {
			var tx Tx
			tx, err := m.Client.NewTrans()
			if err != nil {
				return deltaInfo, err
			}
			defer func() {
				if err == nil {
					err = m.Client.CommitTrans(tx)
				}
				if err != nil {
					m.Client.AbortTrans(tx)
				}
			}()
			err = m.Client.DeleteObjectPart(reqCtx.ObjectInfo, tx)
			if err != nil {
				return deltaInfo, err
			}
			if reqCtx.ObjectInfo.Type == ObjectTypeAppendable && reqCtx.ObjectInfo.Pool == backend.SMALL_FILE_POOLNAME {
				err = m.Client.RemoveHotObject(reqCtx.ObjectInfo, tx)
				if err != nil {
					return deltaInfo, err
				}
			}

			err = m.Client.UpdateObject(object, multipart, updateUsage, tx)
			if err != nil {
				return deltaInfo, err
			}
			if reqCtx.ObjectInfo.StorageClass == object.StorageClass {
				deltaInfo[object.StorageClass] = object.Size - reqCtx.ObjectInfo.Size
			} else {
				deltaInfo[reqCtx.ObjectInfo.StorageClass] = -reqCtx.ObjectInfo.Size
				deltaInfo[object.StorageClass] = object.Size
			}

			return deltaInfo, nil
		} else {
			deltaInfo[object.StorageClass] = object.Size
			return deltaInfo, m.Client.PutObject(object, multipart, updateUsage)
		}
	case datatype.BucketVersioningEnabled:
		deltaInfo[object.StorageClass] = object.Size
		return deltaInfo, m.Client.PutObject(object, multipart, updateUsage)
	default:
		return deltaInfo, ErrInvalidVersioning
	}

}

func (m *Meta) UpdateGlacierObject(reqCtx RequestContext, targetObject, sourceObject *Object) (err error) {
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

	_, err = m.PutObject(reqCtx, targetObject, nil, true)
	if err != nil {
		return err
	}

	err = m.Client.PutObjectToGarbageCollection(sourceObject, tx)
	if err != nil {
		return err
	}

	return err
}

func (m *Meta) UpdateObjectAcl(object *Object) error {
	err := m.Client.UpdateObjectAcl(object)
	return err
}

func (m *Meta) UpdateObjectAttrs(object *Object) error {
	err := m.Client.UpdateObjectAttrs(object)
	return err
}

func (m *Meta) RenameObject(object *Object, sourceObject string) error {
	err := m.Client.RenameObject(object, sourceObject)
	return err
}

func (m *Meta) ReplaceObjectMetas(object *Object) error {
	err := m.Client.ReplaceObjectMetas(object, nil)
	return err
}

func (m *Meta) DeleteObject(object *Object) (err error) {
	tx, err := m.Client.NewTrans()
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

	err = m.Client.DeleteObject(object, tx)
	if err != nil {
		return err
	}

	//delete object meta in hotobjects table
	if object.Type == ObjectTypeAppendable && object.Pool == backend.SMALL_FILE_POOLNAME {
		err = m.Client.RemoveHotObject(object, tx)
		if err != nil {
			return err
		}
	}

	err = m.Client.PutObjectToGarbageCollection(object, tx)
	if err != nil {
		return err
	}

	return m.Client.UpdateUsage(object.BucketName, -object.Size, tx)
}

func (m *Meta) AddDeleteMarker(marker *Object) (err error) {
	return m.Client.PutObject(marker, nil, true)
}

func (m *Meta) DeleteSuspendedObject(object *Object) (err error) {
	tx, err := m.Client.NewTrans()
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

	// only put delete marker if null version does not exist
	if !object.DeleteMarker {
		err = m.Client.DeleteObjectPart(object, tx)
		if err != nil {
			return err
		}

		err = m.Client.PutObjectToGarbageCollection(object, tx)
		if err != nil {
			return err
		}

		err = m.Client.UpdateUsage(object.BucketName, -object.Size, tx)
		if err != nil {
			return err
		}
	}

	// update to delete marker
	object.DeleteMarker = true
	object.LastModifiedTime = time.Now().UTC()
	object.Size = int64(len(object.Name))
	err = m.Client.UpdateObject(object, nil, true, nil)
	if err != nil {
		return err
	}
	return nil
}

func (m *Meta) AppendObject(object *Object, olderObject *Object) error {
	if olderObject == nil {
		return m.Client.AppendObject(object, true)
	} else {
		return m.Client.UpdateAppendObject(object)
	}
}

func (m *Meta) MigrateObject(object *Object) error {
	return m.Client.MigrateObject(object)
}

func (m *Meta) RemoveHotObject(object *Object, tx Tx) error {
	return m.Client.RemoveHotObject(object, tx)
}
