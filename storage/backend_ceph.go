package storage

import (
	"sync"

	"github.com/journeymidnight/yig/backend"
	"github.com/journeymidnight/yig/ceph"
	"github.com/journeymidnight/yig/crypto"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta"
)

func New(metaCacheType int, enableDataCache bool) *YigStorage {
	kms := crypto.NewKMS()
	yig := YigStorage{
		DataStorage: make(map[string]backend.Cluster),
		DataCache:   newDataCache(enableDataCache),
		MetaStorage: meta.New(meta.CacheType(metaCacheType)),
		KMS:         kms,
		Stopping:    false,
		WaitGroup:   new(sync.WaitGroup),
	}

	yig.DataStorage = ceph.Initialize(helper.CONFIG)
	if len(yig.DataStorage) == 0 {
		panic("No data storage can be used!")
	}

	initializeRecycler(&yig)
	return &yig
}
