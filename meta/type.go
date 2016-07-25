package meta

import (
	"github.com/tsuna/gohbase"
	"log"
)

const (
	ZOOKEEPER_ADDRESS = "10.116.77.35:2181,10.116.77.36:2181,10.116.77.37:2181"
	RETRY_LIMIT       = 3

	BUCKET_TABLE         = "buckets"
	BUCKET_COLUMN_FAMILY = "b"
	USER_TABLE           = "users"
	USER_COLUMN_FAMILY   = "u"
	OBJECT_TABLE         = "objects"
	OBJECT_COLUMN_FAMILY = "o"
)

type Meta struct {
	Hbase  gohbase.Client
	Logger *log.Logger
	// TODO Redis and more
}

func New(logger *log.Logger) *Meta {
	hbase := gohbase.NewClient(ZOOKEEPER_ADDRESS)
	meta := Meta{
		Hbase:  hbase,
		Logger: logger,
	}
	return &meta
}

type Bucket struct {
	Name string
	// Date and time when the bucket was created,
	// in format "2006-01-02T15:04:05.000Z"
	CreateTime string
	OwnerId string
	CORS string
	ACL string
}