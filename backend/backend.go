package backend

import (
	"io"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
)

const (
	SMALL_FILE_POOLNAME   = "rabbit"
	BIG_FILE_POOLNAME     = "tiger"
	GLACIER_FILE_POOLNAME = "turtle"
)

type Usage struct {
	UsedSpacePercent int // range 0 ~ 100
}

type Cluster interface {
	// get cluster ID
	ID() string
	// get cluster usage statistics
	GetUsage() (Usage, error)
	// put new object to storage Cluster
	Put(poolName string, object io.Reader) (objectName string,
		bytesWritten uint64, err error)
	// append a new chunk to object, empty existName means new object
	Append(poolName, existName string, objectChunk io.Reader,
		offset int64, length int64) (objectName string, bytesWritten uint64, err error)
	// get a ReadCloser for object, length == 0 means get the whole object
	GetReader(poolName, objectName string,
		offset int64, length uint64) (io.ReadCloser, error)
	// remove an object
	Remove(poolName, objectName string) error
}

// Backend plugins should implement this interface
type Plugin interface {
	// initialize backend cluster handlers,
	// returns cluster ID -> Cluster, panic on errors
	Initialize(logger *log.Logger, config helper.Config) map[string]Cluster
}
