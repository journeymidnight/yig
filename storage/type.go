package storage

import (
	"git.letv.cn/zhangdongmao/radoshttpd/rados"
	"github.com/tsuna/gohbase"
	"log"
)

const (
	CEPH_CONFIG_PATH  = "./conf/ceph.conf"
	MONTIMEOUT        = "10"
	OSDTIMEOUT        = "10"
	ZOOKEEPER_ADDRESS = "10.116.77.35:2181,10.116.77.36:2181,10.116.77.37:2181"

	BUCKET_TABLE         = "buckets"
	BUCKET_COLUMN_FAMILY = "b"
	USER_TABLE           = "users"
	USER_COLUMN_FAMILY   = "u"
	OBJECT_TABLE         = "objects"
	OBJECT_COLUMN_FAMILY = "o"
)

// *YigStorage implements minio.ObjectLayer
type YigStorage struct {
	Rados  *rados.Conn
	Hbase  gohbase.Client
	Logger *log.Logger
	// TODO
}

func New(logger *log.Logger) *YigStorage {
	// you must have admin keyring
	Rados, err := rados.NewConn("admin")
	if err != nil {
		panic("failed to open keyring")
	}

	Rados.SetConfigOption("rados_mon_op_timeout", MONTIMEOUT)
	Rados.SetConfigOption("rados_osd_op_timeout", OSDTIMEOUT)

	err = Rados.ReadConfigFile(CEPH_CONFIG_PATH)
	if err != nil {
		panic("failed to open ceph.conf")
	}

	err = Rados.Connect()
	if err != nil {
		panic("failed to connect to remote cluster")
	}
	defer Rados.Shutdown()

	hbase := gohbase.NewClient(ZOOKEEPER_ADDRESS)

	yig := &YigStorage{
		Rados:  Rados,
		Hbase:  hbase,
		Logger: logger,
	}
	return yig
}
