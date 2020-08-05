package meta

import (
	"10.0.45.221/meepo/kafka.git"
	"github.com/Shopify/sarama"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/client"
	"github.com/journeymidnight/yig/meta/client/tidbclient"
	"github.com/journeymidnight/yig/meta/client/tikvclient"
	"time"
)

type Meta struct {
	Client                    client.Client
	Cache                     MetaCache
	QosMeta                   *QosMeta
	GarbageCollectionProducer kafka.Producer
	republishChannel          chan *sarama.ProducerMessage
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
	meta.republishChannel = make(chan *sarama.ProducerMessage, 1024)
	go func() {
		for {
			e, ok := <-producer.Errors()
			if !ok {
				return
			}
			helper.Logger.Error("Producer error:", e)
			meta.republishChannel <- e.Msg
		}
	}()
	go func() {
		for {
			time.Sleep(time.Second)
			msg := <- meta.republishChannel
			meta.GarbageCollectionProducer.Republish(msg)
		}
	}()
	return &meta
}
