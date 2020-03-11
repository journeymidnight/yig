package types

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/journeymidnight/yig/api/datatype"
)

type Bucket struct {
	Name string
	// Date and time when the bucket was created,
	// should be serialized into format "2006-01-02T15:04:05.000Z"
	CreateTime    time.Time
	OwnerId       string
	CORS          datatype.Cors
	ACL           datatype.Acl
	BucketLogging datatype.BucketLoggingStatus
	Lifecycle     datatype.Lifecycle
	Policy        []byte // need to MarshalJSON
	Website       datatype.WebsiteConfiguration
	Encryption    datatype.EncryptionConfiguration
	Versioning    datatype.BucketVersioningType // actually enum: Disabled/Enabled/Suspended
	Usage         int64
}

func (b *Bucket) String() (s string) {
	s += "Name: " + b.Name + "\t"
	s += "CreateTime: " + b.CreateTime.Format(CREATE_TIME_LAYOUT) + "\t"
	s += "OwnerId: " + b.OwnerId + "\t"
	s += "CORS: " + fmt.Sprintf("%+v", b.CORS) + "\t"
	s += "ACL: " + fmt.Sprintf("%+v", b.ACL) + "\t"
	s += "BucketLogging: " + fmt.Sprintf("%+v", b.BucketLogging) + "\t"
	s += "LifeCycle: " + fmt.Sprintf("%+v", b.Lifecycle) + "\t"
	s += "Policy: " + fmt.Sprintf("%+v", b.Policy) + "\t"
	s += "Website: " + fmt.Sprintf("%+v", b.Website) + "\t"
	s += "Encryption" + fmt.Sprintf("%+v", b.Encryption) + "\t"
	s += "Version: " + b.Versioning.String() + "\t"
	s += "Usage: " + humanize.Bytes(uint64(b.Usage)) + "\t"
	return
}

//Tidb related function
func (b Bucket) GetUpdateSql() (string, []interface{}) {
	acl, _ := json.Marshal(b.ACL)
	cors, _ := json.Marshal(b.CORS)
	logging, _ := json.Marshal(b.BucketLogging)
	lc, _ := json.Marshal(b.Lifecycle)
	website, _ := json.Marshal(b.Website)
	encryption, _ := json.Marshal(b.Encryption)
	sql := "update buckets set bucketname=?,acl=?,policy=?,cors=?,logging=?,lc=?,website=?,encryption=?,uid=?,versioning=? where bucketname=?"
	args := []interface{}{b.Name, acl, b.Policy, cors, logging, lc, website, encryption, b.OwnerId, b.Versioning, b.Name}
	return sql, args
}

func (b Bucket) GetCreateSql() (string, []interface{}) {
	acl, _ := json.Marshal(b.ACL)
	cors, _ := json.Marshal(b.CORS)
	logging, _ := json.Marshal(b.BucketLogging)
	lc, _ := json.Marshal(b.Lifecycle)
	website, _ := json.Marshal(b.Website)
	encryption, _ := json.Marshal(b.Encryption)
	createTime := b.CreateTime.Format(TIME_LAYOUT_TIDB)
	sql := "insert into buckets(bucketname,acl,cors,logging,lc,uid,policy,website,encryption,createtime,usages,versioning) " +
		"values(?,?,?,?,?,?,?,?,?,?,?,?);"
	args := []interface{}{b.Name, acl, cors, logging, lc, b.OwnerId, b.Policy, website, encryption, createTime, b.Usage, b.Versioning}
	return sql, args
}
