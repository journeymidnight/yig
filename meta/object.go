package meta

import (
	"encoding/hex"
	"fmt"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"github.com/minio/highwayhash"
	_ "github.com/minio/highwayhash"
	"io"
	"strings"
)

func (m *Meta) GetObject(bucketName string, objectName string, willNeed bool) (object *Object, err error) {
	getObject := func() (o interface{}, err error) {
		helper.Logger.Println(10, "GetObject CacheMiss. bucket:", bucketName, "object:", objectName)
		object, err := m.Client.GetObject(bucketName, objectName, "")
		if err != nil {
			return
		}
		helper.Debugln("GetObject object.Name:", object.Name)
		if object.Name != objectName {
			err = ErrNoSuchKey
			return
		}
		return object, nil
	}
	unmarshaller := func(in []byte) (interface{}, error) {
		var object Object
		err := helper.MsgPackUnMarshal(in, &object)
		return &object, err
	}

	o, err := m.Cache.Get(redis.ObjectTable, bucketName+":"+HashSum(objectName)+":",
		getObject, unmarshaller, willNeed)
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

func (m *Meta) GetObjectMap(bucketName, objectName string) (objMap *ObjMap, err error) {
	m.Client.GetObjectMap(bucketName, objectName)
	return
}

func (m *Meta) GetObjectVersion(bucketName, objectName, version string, willNeed bool) (object *Object, err error) {
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
	unmarshaller := func(in []byte) (interface{}, error) {
		var object Object
		err := helper.MsgPackUnMarshal(in, &object)
		return &object, err
	}
	o, err := m.Cache.Get(redis.ObjectTable, bucketName+":"+HashSum(objectName)+":"+version,
		getObjectVersion, unmarshaller, willNeed)
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

func (m *Meta) PutObject(object *Object, multipart *Multipart, objMap *ObjMap, updateUsage bool) error {
	tx, err := m.Client.NewTrans()
	defer func() {
		if err != nil {
			m.Client.AbortTrans(tx)
		} else {
			m.Client.CommitTrans(tx)
		}
	}()

	err = m.Client.PutObject(object, tx)
	if err != nil {
		return err
	}

	if objMap != nil {
		err = m.Client.PutObjectMap(objMap, tx)
		if err != nil {
			return err
		}
	}

	if multipart != nil {
		err = m.Client.DeleteMultipart(multipart, tx)
		if err != nil {
			return err
		}
	}

	if updateUsage {
		err = m.Client.UpdateUsage(object.BucketName, object.Size, tx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Meta) PutObjectEntry(object *Object) error {
	err := m.Client.PutObject(object, nil)
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

func (m *Meta) PutObjMapEntry(objMap *ObjMap) error {
	err := m.Client.PutObjectMap(objMap, nil)
	return err
}

func (m *Meta) DeleteObject(object *Object, DeleteMarker bool, objMap *ObjMap) error {
	tx, err := m.Client.NewTrans()
	defer func() {
		if err != nil {
			m.Client.AbortTrans(tx)
		} else {
			m.Client.CommitTrans(tx)
		}
	}()

	err = m.Client.DeleteObject(object, tx)
	if err != nil {
		return err
	}

	if objMap != nil {
		err = m.Client.DeleteObjectMap(objMap, tx)
		if err != nil {
			return err
		}
	}

	if DeleteMarker {
		return nil
	}

	err = m.Client.PutObjectToGarbageCollection(object, tx)
	if err != nil {
		return err
	}

	err = m.Client.UpdateUsage(object.BucketName, -object.Size, tx)
	if err != nil {
		return err
	}

	return err
}

func (m *Meta) AppendObject(object *Object, isExist bool) error {
	if !isExist {
		return m.Client.PutObject(object, nil)
	}
	return m.Client.UpdateAppendObject(object)
}

//Use HighwayHash-256 to compute fingerprints of ObjectName.
func HashSum(ObjectName string) string {
	key, err := hex.DecodeString("000102030405060708090A0B0C0D0E0FF0E0D0C0B0A090807060504030201000") // use your own key here
	if err != nil {
		fmt.Printf("Cannot decode hex key: %v", err) // add error handling
		return "Cannot decode hex key"
	}

	file := strings.NewReader(ObjectName)

	hash, err := highwayhash.New(key)
	if err != nil {
		fmt.Printf("Failed to create HighwayHash instance: %v", err) // add error handling
		return "Failed to create HighwayHash instance"
	}

	if _, err = io.Copy(hash, file); err != nil {
		fmt.Printf("Failed to read from file: %v", err) // add error handling
		return "Failed to read from file"
	}

	sumresult := hex.EncodeToString(hash.Sum(nil))

	return sumresult
}

//func (m *Meta) DeleteObjectEntry(object *Object) error {
//	err := m.Client.DeleteObject(object, nil)
//	return err
//}

//func (m *Meta) DeleteObjMapEntry(objMap *ObjMap) error {
//	err := m.Client.DeleteObjectMap(objMap, nil)
//	return err
//}
