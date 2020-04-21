package types

import "time"

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
	Status           Status
	LifeTime         int
}

func (o *Freezer) GetCreateSql() (string, []interface{}) {
	// TODO Multi-version control
	lastModifiedTime := o.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := "insert into restoreobjects(bucketname,objectname,status,lifetime,lastmodifiedtime) values(?,?,?,?,?)"
	args := []interface{}{o.BucketName, o.Name, o.Status, o.LifeTime, lastModifiedTime}
	return sql, args
}

func (o *Freezer) GetUpdateSql(status Status) (string, []interface{}) {
	// TODO Multi-version control
	// version := math.MaxUint64 - uint64(o.LastModifiedTime.UnixNano())
	lastModifiedTime := o.LastModifiedTime.Format(TIME_LAYOUT_TIDB)
	sql := "update restoreobjects set status=?,lastmodifiedtime=?,location=?,pool=?," +
		"ownerid=?,size=?,etag=? where bucketname=? and objectname=? and status=?"
	args := []interface{}{status, lastModifiedTime, o.Location, o.Pool, o.OwnerId, o.Size, o.Etag, o.BucketName, o.Name, o.Status}

	return sql, args
}
