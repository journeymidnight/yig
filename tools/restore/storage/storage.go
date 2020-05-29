package storage

import (
	"github.com/journeymidnight/yig-restore/backend"
	"github.com/journeymidnight/yig-restore/helper"
	"github.com/journeymidnight/yig-restore/meta"
	"sync"
)

const (
	AES_BLOCK_SIZE = 16
)

type Storage struct {
	DataStorage map[string]backend.Cluster
	MetaStorage *meta.Meta
	Stopping    bool
	WaitGroup   *sync.WaitGroup
}

func (y *Storage) Stop() {
	y.Stopping = true
	helper.Logger.Info("Stopping storage...")
	y.WaitGroup.Wait()
	helper.Logger.Info("done")
}
