package meta

import (
	"github.com/cannium/meepo/kafka"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/client"
	"github.com/journeymidnight/yig/meta/client/tidbclient"
	"github.com/journeymidnight/yig/meta/client/tikvclient"
)

type Meta struct {
	Client                    client.Client
	Cache                     MetaCache
	QosMeta                   *QosMeta
	GarbageCollectionProducer kafka.Producer
}

func New(myCacheType CacheType) *Meta {
	meta := Meta{
		Cache: newMetaCache(myCacheType),
	}
	switch helper.CONFIG.MetaStore {
	case "tidb":
		meta.Client = tidbclient.NewTidbClient()
	case "tikv":
		meta.Client = tikvclient.NewClient(helper.CONFIG.PdAddress)
	default:
		panic("unsupport metastore")
	}
	meta.QosMeta = NewQosMeta(meta.Client)
	producer, err := kafka.NewProducer(helper.CONFIG.KafkaBrokers,
		helper.CONFIG.GcTopic)
	if err != nil {
		panic("new kafka producer: " + err.Error())
	}
	meta.GarbageCollectionProducer = producer
	go func() {
		for {
			e, ok := <-producer.Errors()
			if !ok {
				return
			}
			helper.Logger.Error("Producer error:", e)
		}
	}()
	return &meta
}
