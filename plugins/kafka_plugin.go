package main

import (
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/mods"
	"github.com/journeymidnight/yig/mq/types"
	"strconv"
)

const pluginName = "kafka"

//The variable MUST be named as Exported.
//the code in yig-plugin will lookup this symbol
var Exported = mods.YigPlugin{
	Name:       pluginName,
	PluginType: mods.MQ_PLUGIN,
	Create:     GetKafkaClient,
}

func GetKafkaClient(config map[string]interface{}) (interface{}, error) {
	helper.Logger.Info("Get Kafka plugin config:", config)
	brokerList := ""
	autoOffsetStore := false

	params := config
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

	requestTimeout, _ := strconv.Atoi(config["request_timeout_ms"].(string))
	messageTimeout, _ := strconv.Atoi(config["message_timeout_ms"].(string))
	messageMaxRetries, _ := strconv.Atoi(config["send_max_retries"].(string))

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        brokerList,
		"enable.auto.offset.store": autoOffsetStore,
		"request.timeout.ms":       requestTimeout,
		"message.timeout.ms":       messageTimeout,
		"message.send.max.retries": messageMaxRetries,
	})
	if err != nil {
		return nil, err
	}

	kafka := &Kafka{
		producer: p,
		doneChan: make(chan int),
		Topic:    config["topic"].(string),
	}

	kafka.Start()
	helper.Logger.Info("start kafka")
	return interface{}(kafka), nil
}

type Kafka struct {
	producer *kafka.Producer
	doneChan chan int
	Topic    string
}

func (kf *Kafka) Start() error {
	if kf.producer == nil {
		return errors.New("Kafka sender is not created correctly.")
	}

	go func() {
		defer close(kf.doneChan)
		for e := range kf.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.Opaque != nil {
					switch v := m.Opaque.(type) {
					case chan error:
						go func(c chan error, err error) {
							c <- err
						}(v, m.TopicPartition.Error)
					}
				}
				if m.TopicPartition.Error != nil {
					// error here.
					helper.Logger.Error(
						fmt.Sprintf(
							"Failed to send message to topic[%s] [%d] at offset [%v] with err: %v",
							*m.TopicPartition.Topic, m.TopicPartition.Partition,
							m.TopicPartition.Offset, m.TopicPartition.Error))
					break
				}
				helper.Logger.Info(fmt.Sprintf("Succeed to send message to topic[%s] [%d] at offset [%v]",
					*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset))
			default:
				helper.Logger.Info("Skip event:", ev)
			}
		}
	}()

	return nil
}

func (kf *Kafka) Flush(timeout int) error {
	if nil == kf.producer {
		return errors.New("Kafka sender is not created correclty yet.")
	}
	kf.producer.Flush(timeout)
	return nil
}

func (kf *Kafka) Close() {
	if nil == kf.producer {
		return
	}
	kf.producer.Flush(300000)
	kf.producer.Close()
	_ = <-kf.doneChan
}

func (kf *Kafka) AsyncSend(value []byte) error {
	if nil == kf.producer {
		return errors.New("Kafka is not created correctly yet.")
	}
	if nil == value || "" == kf.Topic {
		return errors.New(fmt.Sprintf("input message[%v] is invalid.", value))
	}
	kf.producer.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &kf.Topic, Partition: kafka.PartitionAny}, Key: []byte(""), Value: value, Opaque: nil}
	return nil
}
