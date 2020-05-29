package storage

import (
	"github.com/journeymidnight/yig-restore/backend"
	"github.com/journeymidnight/yig-restore/ceph"
	"github.com/journeymidnight/yig-restore/helper"
	"github.com/journeymidnight/yig-restore/meta"
	"sync"
)

func New() (STORAGE *Storage) {
	restore := Storage{
		DataStorage: make(map[string]backend.Cluster),
		MetaStorage: meta.New(),
		Stopping:    false,
		WaitGroup:   new(sync.WaitGroup),
	}

	restore.DataStorage = ceph.Initialize(helper.Conf)
	if len(restore.DataStorage) == 0 {
		panic("No data storage can be used!")
	}

	initializeRecycler(&restore)
	return &restore
}
