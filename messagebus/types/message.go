package types

type Message struct {
	Topic   string
	Key     string
	ErrChan chan error
	Value   []byte
}

func NewMsg(topic, key string, val []byte) *Message {
	msg := &Message{
		Topic: topic,
		Key:   key,
		Value: val,
	}
	msg.ErrChan = make(chan error)
	return msg
}
