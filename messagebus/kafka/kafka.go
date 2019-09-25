package kafka

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/messagebus/types"
)

type Kafka struct {
	producer *kafka.Producer
	doneChan chan int
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

func (kf *Kafka) AsyncSend(msg *types.Message) error {
	if nil == kf.producer {
		return errors.New("Kafka is not created correctly yet.")
	}
	if nil == msg.Value || "" == msg.Topic {
		return errors.New(fmt.Sprintf("input message[%v] is invalid.", msg))
	}
	var key []byte
	if msg.Key != "" {
		key = []byte(msg.Key)
	}
	kf.producer.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &msg.Topic, Partition: kafka.PartitionAny}, Key: key, Value: msg.Value, Opaque: msg.ErrChan}
	return nil
}
