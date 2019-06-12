package types

const (
	MSG_BUS_SENDER_UNKNOWN = iota
	MSG_BUS_SENDER_KAFKA
)

const (
	KAFKA_CFG_BROKER_LIST       = "broker_list"
	KAFKA_CFG_AUTO_OFFSET_STORE = "auto_offset_store"
	KAFKA_CFG_MSG_TOPIC         = "topic"
)
