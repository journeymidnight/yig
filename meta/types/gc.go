package types

import (
	"time"
)

type GarbageCollection struct {
	Rowkey     string // rowkey cache
	BucketName string
	ObjectName string
	VersionId  string
	Location   string
	Pool       string
	ObjectId   string
	Status     string    // status of this entry, in Pending/Deleting
	MTime      time.Time // last modify time of status
	Parts      map[int]*Part
	TriedTimes int
	Type       ObjectType
}

func GetGcInfoFromObject(o *Object) (gc GarbageCollection) {
	gc.BucketName = o.BucketName
	gc.ObjectName = o.Name
	gc.Location = o.Location
	gc.Pool = o.Pool
	gc.ObjectId = o.ObjectId
	gc.Status = "Pending"
	gc.MTime = time.Now().UTC()
	gc.Parts = o.Parts
	gc.TriedTimes = 0
	gc.Type = o.Type
	return
}
