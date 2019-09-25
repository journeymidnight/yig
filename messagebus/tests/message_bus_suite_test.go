package tests

import (
	"os"
	"testing"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	bus "github.com/journeymidnight/yig/messagebus"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type MessageBusTestSuite struct {
	logger log.Logger
}

var _ = Suite(&MessageBusTestSuite{})

func (mbs *MessageBusTestSuite) SetUpSuite(c *C) {
	mbs.logger = log.NewLogger(os.Stdout, log.InfoLevel)
	helper.Logger = mbs.logger
}

func (mbs *MessageBusTestSuite) TearDownSuite(c *C) {
}

func (mbs *MessageBusTestSuite) SetUpTest(c *C) {
	helper.CONFIG.MsgBus.Type = 0
	helper.CONFIG.MsgBus.RequestTimeoutMs = 3000
	helper.CONFIG.MsgBus.MessageTimeoutMs = 5000
	helper.CONFIG.MsgBus.SendMaxRetries = 2
	helper.CONFIG.MsgBus.Server = make(map[string]interface{})
	bus.ClearInit()
}

func (mbs *MessageBusTestSuite) TearDownTest(c *C) {
}
