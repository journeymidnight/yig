package types

import (
	"encoding/json"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/policy"
	"time"
)

const (
	VersionEnabled   = "Enabled"
	VersionDisabled  = "Disabled"
	VersionSuspended = "Suspended"
)

type Bucket struct {
	Name string
	// Date and time when the bucket was created,
	// should be serialized into format "2006-01-02T15:04:05.000Z"
	CreateTime time.Time
	OwnerId    string
	CORS       datatype.Cors
	ACL        datatype.Acl
	Lifecycle  datatype.Lifecycle
	Policy     policy.Policy
	Website    datatype.WebsiteConfiguration
	Versioning string // actually enum: Disabled/Enabled/Suspended
	Usage      int64
}

func (b *Bucket) String() (s string) {
	s += "Name: " + b.Name + "\n"
	s += "CreateTime: " + b.CreateTime.Format(CREATE_TIME_LAYOUT) + "\n"
	s += "OwnerId: " + b.OwnerId + "\n"
	s += "CORS: " + fmt.Sprintf("%+v", b.CORS) + "\n"
	s += "ACL: " + fmt.Sprintf("%+v", b.ACL) + "\n"
	s += "LifeCycle: " + fmt.Sprintf("%+v", b.Lifecycle) + "\n"
	s += "Policy: " + fmt.Sprintf("%+v", b.Policy) + "\n"
	s += "Website: " + fmt.Sprintf("%+v", b.Website) + "\n"
	s += "Version: " + b.Versioning + "\n"
	s += "Usage: " + humanize.Bytes(uint64(b.Usage)) + "\n"
	return
}

//Tidb related function
func (b Bucket) GetUpdateSql() (string, []interface{}) {
	acl, _ := json.Marshal(b.ACL)
	cors, _ := json.Marshal(b.CORS)
	lc, _ := json.Marshal(b.Lifecycle)
	bucket_policy, _ := json.Marshal(b.Policy)
	website, _ := json.Marshal(b.Website)
	sql := "update buckets set bucketname=?,acl=?,policy=?,cors=?,lc=?,website=?,uid=?,versioning=? where bucketname=?"
	args := []interface{}{b.Name, acl, bucket_policy, cors, lc, website, b.OwnerId, b.Versioning, b.Name}
	return sql, args
}

func (b Bucket) GetCreateSql() (string, []interface{}) {
	acl, _ := json.Marshal(b.ACL)
	cors, _ := json.Marshal(b.CORS)
	lc, _ := json.Marshal(b.Lifecycle)
	bucket_policy, _ := json.Marshal(b.Policy)
	website, _ := json.Marshal(b.Website)
	createTime := b.CreateTime.Format(TIME_LAYOUT_TIDB)

	sql := "insert into buckets(bucketname,acl,cors,lc,uid,policy,website,createtime,usages,versioning) " +
		"values(?,?,?,?,?,?,?,?,?,?);"
	args := []interface{}{b.Name, acl, cors, lc, b.OwnerId, bucket_policy, website, createTime, b.Usage, b.Versioning}
	return sql, args
}
