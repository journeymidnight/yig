package types

import (
	"time"
)

type GarbageCollection struct {
	Rowkey     string // rowkey cache
	BucketName string
	ObjectName string
	Location   string
	Pool       string
	ObjectId   string
	Status     string    // status of this entry, in Pending/Deleting
	MTime      time.Time // last modify time of status
	Parts      map[int]*Part
	TriedTimes int
}

