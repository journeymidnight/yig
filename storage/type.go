package storage

import (
	"git.letv.cn/yig/yig/meta"
	"git.letv.cn/zhangdongmao/radoshttpd/rados"
	"log"
)

const (
	CEPH_CONFIG_PATH = "./conf/ceph.conf"
	MONTIMEOUT       = "10"
	OSDTIMEOUT       = "10"
)

// *YigStorage implements minio.ObjectLayer
type YigStorage struct {
	DataStorage *rados.Conn
	MetaStorage *meta.Meta
	Logger      *log.Logger
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

	metaStorage := meta.New(logger)

	yig := YigStorage{
		DataStorage: Rados,
		MetaStorage: metaStorage,
		Logger:      logger,
	}
	return &yig
}
