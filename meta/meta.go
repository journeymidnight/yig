package meta

import (
	"errors"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta/client"
	"github.com/journeymidnight/yig/meta/client/tidbclient"
)

const (
	ENCRYPTION_KEY_LENGTH = 32 // 32 bytes for AES-"256"
)

type Meta struct {
	Client client.Client
	Logger *log.Logger
	Cache  MetaCache
}

func (m *Meta) Stop() {
	if m.Cache != nil {
		m.Cache.Close()
	}
}

func (m *Meta) Sync(event SyncEvent) error {
	switch event.Type {
	case SYNC_EVENT_TYPE_BUCKET_USAGE:
		return m.bucketUsageSync()
	default:
		return errors.New("got unknown sync event.")
	}
}

func New(logger *log.Logger, myCacheType CacheType) *Meta {
	meta := Meta{
		Logger: logger,
		Cache:  newMetaCache(myCacheType),
	}
	if helper.CONFIG.MetaStore == "tidb" {
		meta.Client = tidbclient.NewTidbClient()
	} else {
		panic("unsupport metastore")
	}
	if myCacheType != NoCache {
		err := meta.InitBucketUsageCache()
		if err != nil {
			panic("failed to init bucket usage cache")
		}
	}
	return &meta
}
