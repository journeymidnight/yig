package meta

const (
	SYNC_EVENT_TYPE_UNKNOWN = iota
	SYNC_EVENT_TYPE_BUCKET_USAGE
)

type SyncEvent struct {
	Type       int
	RetryTimes int
	Data       interface{}
}

type SyncWorker interface {
	Sync(event SyncEvent) error
}

var MetaSyncQueue chan SyncEvent
