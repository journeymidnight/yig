package types

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/policy"
	"github.com/journeymidnight/yig/helper"
)

const (
	FIELD_NAME_BODY       = "body"
	FIELD_NAME_USAGE      = "usage"
	FIELD_NAME_FILECOUNTS = "file_counts"
)

type Bucket struct {
	Name string
	// Date and time when the bucket was created,
	// should be serialized into format "2006-01-02T15:04:05.000Z"
	CreateTime time.Time
	OwnerId    string
	CORS       datatype.Cors
	ACL        datatype.Acl
	LC         datatype.Lc
	Policy     policy.Policy
	Versioning string // actually enum: Disabled/Enabled/Suspended
	Usage      int64
	FileCounts int64
	UpdateTime time.Time
}

// implements the Serializable interface
func (b *Bucket) Serialize() (map[string]interface{}, error) {
	fields := make(map[string]interface{})
	bytes, err := helper.MsgPackMarshal(b)
	if err != nil {
		return nil, err
	}
	fields[FIELD_NAME_BODY] = string(bytes)
	fields[FIELD_NAME_USAGE] = b.Usage
	fields[FIELD_NAME_FILECOUNTS] = b.FileCounts
	return fields, nil
}

func (b *Bucket) Deserialize(fields map[string]string) (interface{}, error) {
	body, ok := fields[FIELD_NAME_BODY]
	if !ok {
		return nil, errors.New(fmt.Sprintf("no field %s found", FIELD_NAME_BODY))
	}

	err := helper.MsgPackUnMarshal([]byte(body), b)
	if err != nil {
		return nil, err
	}
	if usageStr, ok := fields[FIELD_NAME_USAGE]; ok {
		b.Usage, err = strconv.ParseInt(usageStr, 10, 64)
		if err != nil {
			return nil, err
		}
	}

	if fileCountStr, ok := fields[FIELD_NAME_FILECOUNTS]; ok {
		b.FileCounts, err = strconv.ParseInt(fileCountStr, 10, 64)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

func (b *Bucket) String() (s string) {
	s += "Name: " + b.Name + "\n"
	s += "CreateTime: " + b.CreateTime.Format(CREATE_TIME_LAYOUT) + "\n"
	s += "OwnerId: " + b.OwnerId + "\n"
	s += "CORS: " + fmt.Sprintf("%+v", b.CORS) + "\n"
	s += "ACL: " + fmt.Sprintf("%+v", b.ACL) + "\n"
	s += "LifeCycle: " + fmt.Sprintf("%+v", b.LC) + "\n"
	s += "Policy: " + fmt.Sprintf("%+v", b.Policy) + "\n"
	s += "Version: " + b.Versioning + "\n"
	s += "Usage: " + humanize.Bytes(uint64(b.Usage)) + "\n"
	s += "FileCounts: " + humanize.Bytes(uint64(b.FileCounts)) + "\n"
	s += "UpdateTime: " + b.UpdateTime.Format(CREATE_TIME_LAYOUT) + "\n"
	return
}

/* Learn from this, http://stackoverflow.com/questions/33587227/golang-method-sets-pointer-vs-value-receiver */
/* If you have a T and it is addressable you can call methods that have a receiver type of *T as well as methods that have a receiver type of T */
func (b *Bucket) GetValues() (values map[string]map[string][]byte, err error) {
	cors, err := json.Marshal(b.CORS)
	if err != nil {
		return
	}
	lc, err := json.Marshal(b.LC)
	if err != nil {
		return
	}
	var usage bytes.Buffer
	err = binary.Write(&usage, binary.BigEndian, b.Usage)
	if err != nil {
		return
	}
	var fileCounts bytes.Buffer
	err = binary.Write(&fileCounts, binary.BigEndian, b.FileCounts)
	if err != nil {
		return
	}
	values = map[string]map[string][]byte{
		BUCKET_COLUMN_FAMILY: map[string][]byte{
			"UID":        []byte(b.OwnerId),
			"ACL":        []byte(b.ACL.CannedAcl),
			"CORS":       cors,
			"LC":         lc,
			"createTime": []byte(b.CreateTime.Format(CREATE_TIME_LAYOUT)),
			"versioning": []byte(b.Versioning),
			"usage":      usage.Bytes(),
			"FileCounts": fileCounts.Bytes(),
			"UpdateTime": []byte(b.UpdateTime.Format(CREATE_TIME_LAYOUT)),
		},
		// TODO fancy ACL
	}
	return
}

//Tidb related function
func (b Bucket) GetUpdateSql() (string, []interface{}) {
	acl, _ := json.Marshal(b.ACL)
	cors, _ := json.Marshal(b.CORS)
	lc, _ := json.Marshal(b.LC)
	bucket_policy, _ := json.Marshal(b.Policy)
	sql := "update buckets set bucketname=?,acl=?,policy=?,cors=?,lc=?,uid=?,versioning=?,file_counts=? where bucketname=?"
	args := []interface{}{b.Name, acl, bucket_policy, cors, lc, b.OwnerId, b.Versioning, b.FileCounts, b.Name}
	return sql, args
}

func (b Bucket) GetCreateSql() (string, []interface{}) {
	acl, _ := json.Marshal(b.ACL)
	cors, _ := json.Marshal(b.CORS)
	lc, _ := json.Marshal(b.LC)
	bucket_policy, _ := json.Marshal(b.Policy)
	createTime := b.CreateTime.Format(TIME_LAYOUT_TIDB)

	sql := "insert into buckets(bucketname,acl,cors,lc,uid,policy,createtime,usages,versioning,file_counts) " +
		"values(?,?,?,?,?,?,?,?,?,?);"
	args := []interface{}{b.Name, acl, cors, lc, b.OwnerId, bucket_policy, createTime, b.Usage, b.Versioning, b.FileCounts}
	return sql, args
}
