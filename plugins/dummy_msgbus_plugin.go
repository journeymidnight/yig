package main

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/mods"
)

const pluginName = "dummy_msgbus"

//The variable MUST be named as Exported.
//the code in yig-plugin will lookup this symbol
var Exported = mods.YigPlugin{
	Name:       pluginName,
	PluginType: mods.MESSAGEBUS_PLUGIN,
	Create:     GetDummyMsgBusClient,
}

func GetDummyMsgBusClient(config map[string]interface{}) (interface{}, error) {
	msgbus := dummyMsgBus{
		Url:   config["url"].(string),
		Topic: config["topic"].(string),
	}
	return interface{}(msgbus), nil
}

type dummyMsgBus struct {
	Url   string
	Topic string
}

func (mb *dummyMsgBus) Start() error {
	return nil
}

func (mb *dummyMsgBus) Flush(timeout int) error {
	return nil
}

func (mb *dummyMsgBus) Close() {

}

func (mb *dummyMsgBus) AsyncSend(value []byte) error {
	helper.Logger.Println("Send message succeed! url is:", mb.Url, "topic is:", mb.Topic)
	return nil
}
