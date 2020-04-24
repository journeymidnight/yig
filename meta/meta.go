package meta

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/client"
	"github.com/journeymidnight/yig/meta/client/tidbclient"
)

const (
	ENCRYPTION_KEY_LENGTH = 32 // 32 bytes for AES-"256"
)

type Meta struct {
	Client  client.Client
	Cache   MetaCache
	QosMeta *QosMeta
}

func New(myCacheType CacheType) *Meta {
	meta := Meta{
		Cache: newMetaCache(myCacheType),
	}
	if helper.CONFIG.MetaStore == "tidb" {
		meta.Client = tidbclient.NewTidbClient()
	} else {
		panic("unsupport metastore")
	}
	meta.QosMeta = NewQosMeta(meta.Client)
	return &meta
}
