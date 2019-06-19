package kafka

import (
	"errors"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/journeymidnight/yig/helper"
	bus "github.com/journeymidnight/yig/messagebus"
	"github.com/journeymidnight/yig/messagebus/types"
)

type KafkaBuilder struct {
}

func (kb *KafkaBuilder) Create(config helper.MsgBusConfig) (bus.MessageSender, error) {
	brokerList := ""
	autoOffsetStore := false

	params := config.Server
	if nil == params || len(params) == 0 {
		return nil, errors.New("input kafka params is invalid")
	}

	// get broker list
	v, ok := params[types.KAFKA_CFG_BROKER_LIST]
	if !ok {
		return nil, errors.New("params doesn't contain validate broker list")
	}
	brokerList = v.(string)

	// try to get the enable.auto.offset.store
	if v, ok = params[types.KAFKA_CFG_AUTO_OFFSET_STORE]; ok {
		autoOffsetStore = v.(bool)
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        brokerList,
		"enable.auto.offset.store": autoOffsetStore,
		"request.timeout.ms":       config.RequestTimeoutMs,
		"message.timeout.ms":       config.MessageTimeoutMs,
		"message.send.max.retries": config.SendMaxRetries,
	})
	if err != nil {
		return nil, err
	}

	kafka := &Kafka{
		producer: p,
		doneChan: make(chan int),
	}

	kafka.Start()

	return kafka, nil
}
