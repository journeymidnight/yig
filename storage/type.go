package storage

import (
	"git.letv.cn/ceph/radoshttpd/rados"
	"git.letv.cn/yig/yig/meta"
	"log"
)

const (
	CEPH_CONFIG_PATH     = "./conf/ceph.conf"
	BIG_FILE_POOL_NAME   = "big"
	SMALL_FILE_POOL_NAME = "small"
	BIG_FILE_THRESHOLD   = 1 << 20
	BUFFER_SIZE          = 1 << 20
	MONTIMEOUT           = "10"
	OSDTIMEOUT           = "10"
	STRIPE_UNIT          = uint(512 << 10) /* 512K */
	OBJECT_SIZE          = uint(4 << 20)   /* 4M */
	STRIPE_COUNT         = uint(4)
)

// indicates a Ceph cluster
type CephStorage struct {
	RadosConnection *rados.Conn
	BigFilePool     *rados.Pool
	SmallFilePool   *rados.Pool
	// TODO
}

func NewCephStorage() CephStorage {
	// you must have admin keyring
	rados, err := rados.NewConn("admin")
	if err != nil {
		panic("Failed to open keyring file")
	}

	rados.SetConfigOption("rados_mon_op_timeout", MONTIMEOUT)
	rados.SetConfigOption("rados_osd_op_timeout", OSDTIMEOUT)

	err = rados.ReadConfigFile(CEPH_CONFIG_PATH)
	if err != nil {
		panic("failed to open ceph.conf")
	}

	err = rados.Connect()
	if err != nil {
		panic("Failed to connect to Ceph cluster")
	}

	bigFilePool, err := rados.OpenPool(BIG_FILE_POOL_NAME)
	if err != nil {
		panic("Failed to open big file pool")
	}
	smallFilePool, err := rados.OpenPool(SMALL_FILE_POOL_NAME)
	if err != nil {
		panic("Failed to open small file pool")
	}

	return CephStorage{
		RadosConnection: rados,
		BigFilePool:     bigFilePool,
		SmallFilePool:   smallFilePool,
	}
}

func (c CephStorage) Shutdown() {
	c.RadosConnection.Shutdown()
}

// *YigStorage implements minio.ObjectLayer
type YigStorage struct {
	DataStorage CephStorage
	MetaStorage *meta.Meta
	Logger      *log.Logger
	// TODO
}

func New(logger *log.Logger) *YigStorage {
	dataStorage := NewCephStorage()
	metaStorage := meta.New(logger)

	yig := YigStorage{
		DataStorage: dataStorage,
		MetaStorage: metaStorage,
		Logger:      logger,
	}
	return &yig
}

type IoContextWrapper struct {
	oid     string
	striper *rados.StriperPool
	offset  int
}

func (wrapper IoContextWrapper) Write(d []byte) (int, error) {
	n, err := wrapper.striper.Write(wrapper.oid, d, uint64(wrapper.offset))

	if err != nil {
		return n, err
	} else {
		wrapper.offset += len(d)
	}
	return len(d), err
}
