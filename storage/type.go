package storage

import (
	"git.letv.cn/yig/yig/meta"
	"log"
	"path/filepath"
)

const (
	CEPH_CONFIG_PATTERN  = "conf/*.conf"
)

// *YigStorage implements minio.ObjectLayer
type YigStorage struct {
	DataStorage map[string]*CephStorage
	MetaStorage *meta.Meta
	Logger      *log.Logger
	// TODO
}

func New(logger *log.Logger) *YigStorage {
	metaStorage := meta.New(logger)
	yig := YigStorage{
		DataStorage:make(map[string]*CephStorage),
		MetaStorage: metaStorage,
		Logger:      logger,
	}

	cephConfs, err  := filepath.Glob(CEPH_CONFIG_PATTERN)
	if err != nil {
		panic("No ceph conf found")
	}

	for _, conf := range cephConfs {
		c := NewCephStorage(conf, logger)
		yig.DataStorage[c.Name]=c
	}

	return &yig
}

