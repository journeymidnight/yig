package storage

import (
	"git.letv.cn/yig/yig/minio/datatype"
	"io"
)



func (yig *YigStorage) PickOneClusterAndPool(bucket string, object string, size int64) (cluster *CephStorage, poolName string) {
	//TODO choose the first cluster for testing
	if size > (512 << 10) {
		return yig.DataStorage["2fc32752-04a3-48dc-8297-40fb4dd11ff5"], POOLNAME_TIGER
	} else {
		return yig.DataStorage["2fc32752-04a3-48dc-8297-40fb4dd11ff5"], POOLNAME_RABBIT
	}
}

func (yig *YigStorage) GetObject(bucket, object string, startOffset int64, length int64, writer io.Writer) (err error) {
	return
}

func (yig *YigStorage) GetObjectInfo(bucket, object string) (objInfo datatype.ObjectInfo, err error) {
	return
}

func (yig *YigStorage) PutObject(bucket, object string, size int64, data io.Reader, metadata map[string]string) (md5 string, err error) {

	ceph, poolname := yig.PickOneClusterAndPool(bucket, object, size)

	//Mapping a shorter name for the object
	oid := ceph.GetUniqUploadName()

	ceph.put(poolname, oid, data)
	return
}

func (yig *YigStorage) DeleteObject(bucket, object string) error {
	return nil
}

