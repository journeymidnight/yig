package types

import (
	"time"

	. "github.com/journeymidnight/yig/meta/common"
)

type Freezer struct {
	Name             string
	BucketName       string
	VersionId        string    // version cache
	Location         string    // which Ceph cluster this object locates
	Pool             string    // which Ceph pool this object locates
	Size             int64     // file size
	ObjectId         string    // object name in Ceph
	LastModifiedTime time.Time // in format "2006-01-02T15:04:05.000Z"
	Parts            map[int]*Part
	PartsIndex       *SimpleIndex
	Status           RestoreStatus
	LifeTime         int
	Type             ObjectType
	CreateTime       uint64 // Timestamp(nanosecond)
}

func (o *Freezer) GetCreateSql() (string, []interface{}) {
	lastModifiedTime := o.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := "insert into restoreobjects(bucketname,objectname,version,status,lifetime,lastmodifiedtime) values(?,?,?,?,?,?)"
	args := []interface{}{o.BucketName, o.Name, o.VersionId, o.Status, o.LifeTime, lastModifiedTime}
	return sql, args
}

func (o *Freezer) GetUpdateSql(status RestoreStatus) (string, []interface{}) {
	lastModifiedTime := o.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := "update restoreobjects set status=?,lastmodifiedtime=?,location=?,pool=?," +
		"size=?,objectid=? where bucketname=? and objectname=? and version=? and status=?"
	args := []interface{}{status, lastModifiedTime, o.Location, o.Pool, o.Size, o.ObjectId, o.BucketName, o.Name, o.VersionId, o.Status}

	return sql, args
}

func (f *Freezer) ToObject() (o Object) {
	o.Name = f.Name
	o.BucketName = f.BucketName
	o.Location = f.Location
	o.Pool = f.Pool
	o.Size = f.Size
	o.ObjectId = f.ObjectId
	o.LastModifiedTime = f.LastModifiedTime
	o.Parts = f.Parts
	o.PartsIndex = f.PartsIndex
	o.VersionId = f.VersionId
	o.CreateTime = f.CreateTime
	return
}
