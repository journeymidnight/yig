package types

import (
	"time"

	. "github.com/journeymidnight/yig/meta/common"
)

type Freezer struct {
	Rowkey           []byte // Rowkey cache
	Name             string
	BucketName       string
	Location         string // which Ceph cluster this object locates
	Pool             string // which Ceph pool this object locates
	OwnerId          string
	Size             int64     // file size
	ObjectId         string    // object name in Ceph
	LastModifiedTime time.Time // in format "2006-01-02T15:04:05.000Z"
	Etag             string
	Parts            map[int]*Part
	PartsIndex       *SimpleIndex
	VersionId        string // version cache
	Status           RestoreStatus
	LifeTime         int
	Type             ObjectType
	CreateTime       uint64 // Timestamp(nanosecond)
}

func (o *Freezer) GetCreateSql() (string, []interface{}) {
	lastModifiedTime := o.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := "insert into restoreobjects(bucketname,objectname,version,status,lifetime,lastmodifiedtime,type,createtime) values(?,?,?,?,?,?,?,?)"
	args := []interface{}{o.BucketName, o.Name, o.VersionId, o.Status, o.LifeTime, lastModifiedTime, o.Type, o.CreateTime}
	return sql, args
}

func (o *Freezer) GetUpdateSql(status RestoreStatus) (string, []interface{}) {
	lastModifiedTime := o.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := "update restoreobjects set status=?,lastmodifiedtime=?,location=?,pool=?," +
		"ownerid=?,size=?,etag=? where bucketname=? and objectname=? and version=? and status=?"
	args := []interface{}{status, lastModifiedTime, o.Location, o.Pool, o.OwnerId, o.Size, o.Etag, o.BucketName, o.Name, o.VersionId, o.Status}

	return sql, args
}

func (f *Freezer) ToObject() (o Object) {
	o.Rowkey = f.Rowkey
	o.Name = f.Name
	o.BucketName = f.BucketName
	o.Location = f.Location
	o.Pool = f.Pool
	o.OwnerId = f.OwnerId
	o.Size = f.Size
	o.ObjectId = f.ObjectId
	o.LastModifiedTime = f.LastModifiedTime
	o.Etag = f.Etag
	o.Parts = f.Parts
	o.PartsIndex = f.PartsIndex
	o.VersionId = f.VersionId
	return
}
