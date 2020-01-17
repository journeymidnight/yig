package main

import (
	"fmt"
	"github.com/journeymidnight/yig/mods"
)

const pluginName = "dummy_mq"

//The variable MUST be named as Exported.
//the code in yig-plugin will lookup this symbol
var Exported = mods.YigPlugin{
	Name:       pluginName,
	PluginType: mods.MQ_PLUGIN,
	Create:     GetDummyMsgQueueClient,
}

func GetDummyMsgQueueClient(config map[string]interface{}) (interface{}, error) {
	msgQueue := &dummyMsgQueue{
		Url:   config["url"].(string),
		Topic: config["topic"].(string),
	}
	msgQueue.Start()
	return interface{}(msgQueue), nil
}

type dummyMsgQueue struct {
	Url   string
	Topic string
}

func (mb *dummyMsgQueue) Start() error {
	fmt.Println("Start message queue succeed! url is:", mb.Url, "topic is:", mb.Topic)
	return nil
}

func (mb *dummyMsgQueue) Flush(timeout int) error {
	fmt.Println("Flush message queue succeed! url is:", mb.Url, "topic is:", mb.Topic)
	return nil
}

func (mb *dummyMsgQueue) Close() {
	fmt.Println("Close message queue succeed! url is:", mb.Url, "topic is:", mb.Topic)
}

func (mb *dummyMsgQueue) AsyncSend(value []byte) error {
	fmt.Println("Send message succeed! url is:", mb.Url, "topic is:", mb.Topic, "value is：", value)
	return nil
}
