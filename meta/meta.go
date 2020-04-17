package meta

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/client"
	"github.com/journeymidnight/yig/meta/client/tidbclient"
	"github.com/journeymidnight/yig/meta/client/tikvclient"
)

const (
	ENCRYPTION_KEY_LENGTH = 32 // 32 bytes for AES-"256"
)

type Meta struct {
	Client client.Client
	Cache  MetaCache
}

func New(myCacheType CacheType) *Meta {
	meta := Meta{
		Cache: newMetaCache(myCacheType),
	}
	switch helper.CONFIG.MetaStore {
	case "tidb":
		meta.Client = tidbclient.NewTidbClient()
	case "tikv":
		meta.Client = tikvclient.NewClient()
	default:
		panic("unsupport metastore")
	}
	return &meta
}
