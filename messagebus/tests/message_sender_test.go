package tests

import (
	"fmt"

	"github.com/journeymidnight/yig/helper"
	bus "github.com/journeymidnight/yig/messagebus"
	_ "github.com/journeymidnight/yig/messagebus/kafka"
	"github.com/journeymidnight/yig/messagebus/types"
	. "gopkg.in/check.v1"
)

func (mbs *MessageBusTestSuite) TestMessageSenderGet(c *C) {
	helper.CONFIG.MsgBus.Type = 1
	sender, err := bus.GetMessageSender()
	c.Assert(err, Not(Equals), nil)
	c.Assert(sender, Equals, nil)
	helper.CONFIG.MsgBus.Type = 0
	helper.CONFIG.MsgBus.Server[types.KAFKA_CFG_BROKER_LIST] = "localhost:9092"
	sender, err = bus.GetMessageSender()
	c.Assert(err, Not(Equals), nil)
	c.Assert(sender, Equals, nil)

	helper.CONFIG.MsgBus.Type = 1
	helper.CONFIG.MsgBus.Server[types.KAFKA_CFG_BROKER_LIST] = "localhost:9092"
	sender, err = bus.GetMessageSender()
	c.Assert(err, Equals, nil)
	c.Assert(sender, Not(Equals), nil)
	sender.Close()
}

func (mbs *MessageBusTestSuite) TestMessageSendMsgWithInvalidBroker(c *C) {
	helper.CONFIG.MsgBus.Type = 1
	helper.CONFIG.MsgBus.Server[types.KAFKA_CFG_BROKER_LIST] = "non-exist:29092"
	sender, err := bus.GetMessageSender()
	c.Assert(err, Equals, nil)
	c.Assert(sender, Not(Equals), nil)
	defer sender.Close()
	msg := types.NewMsg("testTopic2", "", []byte("hello this is a test"))
	err = sender.AsyncSend(msg)
	c.Assert(err, Equals, nil)
	err = <-msg.ErrChan
	c.Assert(err, Not(Equals), nil)
}

func (mbs *MessageBusTestSuite) TestMessageSendMsgSuccess(c *C) {
	helper.CONFIG.MsgBus.Type = 1
	helper.CONFIG.MsgBus.Server[types.KAFKA_CFG_BROKER_LIST] = "localhost:9092"
	sender, err := bus.GetMessageSender()
	c.Assert(err, Equals, nil)
	c.Assert(sender, Not(Equals), nil)
	defer sender.Close()
	msg := types.NewMsg("testTopic2", "", []byte("hello this is a test"))
	err = sender.AsyncSend(msg)
	c.Assert(err, Equals, nil)
	err = <-msg.ErrChan
	c.Assert(err, Equals, nil)
}

func (mbs *MessageBusTestSuite) TestMessageSendMsgConcurrent(c *C) {
	helper.CONFIG.MsgBus.Type = 1
	helper.CONFIG.MsgBus.Server[types.KAFKA_CFG_BROKER_LIST] = "localhost:9092"
	count := 100
	msgs := make(chan *types.Message, count)
	sender, err := bus.GetMessageSender()
	c.Assert(err, Equals, nil)
	c.Assert(sender, Not(Equals), nil)
	defer sender.Close()
	defer close(msgs)
	for i := 0; i < count; i++ {
		go func(idx int) {
			msg := types.NewMsg("testTopic2", "", []byte(fmt.Sprintf("hello this is a test of %d", idx+1)))
			err = sender.AsyncSend(msg)
			c.Assert(err, Equals, nil)
			msgs <- msg
		}(i)
	}
	for i := 0; i < count; i++ {
		msg := <-msgs
		err = <-msg.ErrChan
		c.Assert(err, Equals, nil)
	}
}

func (mbs *MessageBusTestSuite) BenchmarkSendOneMsg(c *C) {
	helper.CONFIG.MsgBus.Type = 1
	helper.CONFIG.MsgBus.Server[types.KAFKA_CFG_BROKER_LIST] = "localhost:9092"
	sender, err := bus.GetMessageSender()
	c.Assert(err, Equals, nil)
	c.Assert(sender, Not(Equals), nil)
	defer sender.Close()
	for i := 0; i < c.N; i++ {
		msg := types.NewMsg("testTopic2", "", []byte("benchmark test message."))
		defer close(msg.ErrChan)
		err = sender.AsyncSend(msg)
		c.Assert(err, Equals, nil)
		err = <-msg.ErrChan
		c.Assert(err, Equals, nil)
	}
}
