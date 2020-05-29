package types

import (
	"github.com/dustin/go-humanize"
	"github.com/journeymidnight/yig-restore/restore/datatype"
	"time"
)

type Bucket struct {
	Name string
	// Date and time when the bucket was created,
	// should be serialized into format "2006-01-02T15:04:05.000Z"
	CreateTime time.Time
	OwnerId    string
	Versioning datatype.BucketVersioningType // actually enum: Disabled/Enabled/Suspended
	Usage      int64
}

func (b *Bucket) String() (s string) {
	s += "Name: " + b.Name + "\t"
	s += "CreateTime: " + b.CreateTime.Format(CREATE_TIME_LAYOUT) + "\t"
	s += "OwnerId: " + b.OwnerId + "\t"
	s += "Version: " + b.Versioning.String() + "\t"
	s += "Usage: " + humanize.Bytes(uint64(b.Usage)) + "\t"
	return
}
