package kafka

import (
	bus "github.com/journeymidnight/yig/messagebus"
	"github.com/journeymidnight/yig/messagebus/types"
)

func init() {
	kafkaBuilder := &KafkaBuilder{}

	bus.AddMsgBuilder(types.MSG_BUS_SENDER_KAFKA, kafkaBuilder)
}
