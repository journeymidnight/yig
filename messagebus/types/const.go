package types

const (
	MSG_BUS_SENDER_UNKNOWN = iota
	MSG_BUS_SENDER_KAFKA
)

const (
	MSG_BUS_REQUEST_TIMEOUT  = "request.timeout.ms"
	MSG_BUS_MESSAGE_TIMEOUT  = "message.timeout.ms"
	MSG_BUS_SEND_MAX_RETRIES = "send.max.retries"
)

const (
	KAFKA_CFG_BROKER_LIST       = "broker_list"
	KAFKA_CFG_AUTO_OFFSET_STORE = "auto_offset_store"
	KAFKA_CFG_MSG_TOPIC         = "topic"
)
