package messagebus

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/messagebus/types"
	"github.com/journeymidnight/yig/mods"
)

type MessageSender interface {
	// send the message async
	AsyncSend(msg *types.Message) error
	// flush all the messages, timeout is in ms.
	Flush(timeout int) error
	// free this instance.
	Close()
}

var MsgSender MessageSender

// create the singleton MessageSender
func GetMessageSender() (MessageSender, error) {
	plugins := mods.InitialPlugins()
	name := "kafka"
	p := plugins[name]
	c, err := p.Create(helper.CONFIG.Plugins[name].Args)
	if err != nil {
		helper.Logger.Error("failed to initial message bus plugin:", name, "\nerr:", err)
		return nil, err
	}
	helper.Logger.Println("Message bus plugin is", name)
	MsgSender = c.(MessageSender)
	return MsgSender, nil
}
