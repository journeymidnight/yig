package storage

import (
	"git.letv.cn/yig/yig/meta"
	"log"
	"path/filepath"
)


// *YigStorage implements minio.ObjectLayer
type YigStorage struct {
	DataStorage map[string]*CephStorage
	MetaStorage *meta.Meta
	Logger      *log.Logger
	// TODO
}

func New(logger *log.Logger) *YigStorage {
	// you must have admin keyring

	metaStorage := meta.New(logger)


	yig := YigStorage{
		MetaStorage: metaStorage,
		Logger:      logger,
	}
	yig.DataStorage = make(map[string]*CephStorage) 

	cephConfs, err  := filepath.Glob("conf/*.conf")
	if err != nil {
		panic("no ceph conf found")
	}


	for _, conf := range cephConfs {
		c := NewCephStorage(conf,logger)
		yig.DataStorage[c.Name]=c
	}

	return &yig
}
