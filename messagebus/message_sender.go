package messagebus

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/messagebus/types"
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
var initialized uint32
var mu sync.Mutex

// create the singleton MessageSender
func GetMessageSender() (MessageSender, error) {
	var err error
	if atomic.LoadUint32(&initialized) == 1 {
		return MsgSender, nil
	}
	mu.Lock()
	defer mu.Unlock()
	if initialized == 0 {
		builder, ok := MsgBuilders[helper.CONFIG.MsgBus.Type]
		if !ok {
			return nil, errors.New("msg_bus config is invalidate.")
		}
		MsgSender, err = builder.Create(helper.CONFIG.MsgBus.Server)
		if err != nil || nil == MsgSender {
			return nil, errors.New(fmt.Sprintf("failed to create message_sender with err: %v", err))
		}

		atomic.StoreUint32(&initialized, 1)
	}
	return MsgSender, nil
}

// this func is just for testing. Don't use it in other place.
func ClearInit() {
	atomic.StoreUint32(&initialized, 0)
}
