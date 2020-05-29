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
	return
}

func GarbageCollectionFromFreeze(f *Freezer) (gc GarbageCollection) {
	gc.BucketName = f.BucketName
	gc.ObjectName = f.Name
	gc.Location = f.Location
	gc.Pool = f.Pool
	gc.ObjectId = f.ObjectId
	gc.Status = "Pending"
	gc.MTime = time.Now().UTC()
	gc.Parts = f.Parts
	gc.TriedTimes = 0
	return
}
