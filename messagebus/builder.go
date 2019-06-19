package messagebus

import (
	"github.com/journeymidnight/yig/helper"
)

type MessageSenderBuilder interface {
	Create(config helper.MsgBusConfig) (MessageSender, error)
}

var MsgBuilders = make(map[int]MessageSenderBuilder)

func AddMsgBuilder(builderType int, builder MessageSenderBuilder) {
	MsgBuilders[builderType] = builder
}
