package messagebus

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/mods"
)

type MessageSender interface {
	// send the message async
	AsyncSend(value []byte) error
	// flush all the messages, timeout is in ms.
	Flush(timeout int) error
	// free this instance.
	Close()
}

var MsgSender MessageSender

// create the singleton MessageSender
func InitMessageSender(plugins map[string]*mods.YigPlugin) (MessageSender, error) {
	for name, p := range plugins {
		if p.PluginType == mods.MQ_PLUGIN {
			c, err := p.Create(helper.CONFIG.Plugins[name].Args)
			if err != nil {
				helper.Logger.Error("failed to initial message Queue plugin:", name, "\nerr:", err)
				return nil, err
			}
			helper.Logger.Println("Message Queue plugin is", name)
			MsgSender = c.(MessageSender)
			return MsgSender, nil
		}
	}
	panic("Failed to initialize any MessageQueue plugin, quiting...\n")
}
