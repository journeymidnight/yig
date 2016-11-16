package meta

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/redis"
	"github.com/cannium/gohbase/hrpc"
	"time"
)

type Bucket struct {
	Name string
	// Date and time when the bucket was created,
	// should be serialized into format "2006-01-02T15:04:05.000Z"
	CreateTime time.Time
	OwnerId    string
	CORS       datatype.Cors
	ACL        datatype.Acl
	Versioning string // actually enum: Disabled/Enabled/Suspended
	Usage      int64
}

func (b Bucket) GetValues() (values map[string]map[string][]byte, err error) {
	cors, err := json.Marshal(b.CORS)
	if err != nil {
		return
	}
	var usage bytes.Buffer
	err = binary.Write(&usage, binary.BigEndian, b.Usage)
	if err != nil {
		return
	}
	values = map[string]map[string][]byte{
		BUCKET_COLUMN_FAMILY: map[string][]byte{
			"UID":        []byte(b.OwnerId),
			"ACL":        []byte(b.ACL.CannedAcl),
			"CORS":       cors,
			"createTime": []byte(b.CreateTime.Format(CREATE_TIME_LAYOUT)),
			"versioning": []byte(b.Versioning),
			"usage":      usage.Bytes(),
		},
		// TODO fancy ACL
	}
	return
}

func (m *Meta) GetBucket(bucketName string) (bucket Bucket, err error) {
	getBucket := func() (b interface{}, err error) {
		getRequest, err := hrpc.NewGetStr(context.Background(), BUCKET_TABLE, bucketName)
		if err != nil {
			return
		}
		response, err := m.Hbase.Get(getRequest)
		if err != nil {
			m.Logger.Println("Error getting bucket info, with error ", err)
			return
		}
		if len(response.Cells) == 0 {
			err = ErrNoSuchBucket
			return
		}
		var bucket Bucket
		for _, cell := range response.Cells {
			switch string(cell.Qualifier) {
			case "createTime":
				bucket.CreateTime, err = time.Parse(CREATE_TIME_LAYOUT, string(cell.Value))
				if err != nil {
					return
				}
			case "UID":
				bucket.OwnerId = string(cell.Value)
			case "CORS":
				var cors datatype.Cors
				err = json.Unmarshal(cell.Value, &cors)
				if err != nil {
					return
				}
				bucket.CORS = cors
			case "ACL":
				bucket.ACL.CannedAcl = string(cell.Value)
			case "versioning":
				bucket.Versioning = string(cell.Value)
			case "usage":
				err = binary.Read(bytes.NewReader(cell.Value), binary.BigEndian,
					&bucket.Usage)
				if err != nil {
					return
				}
			default:
			}
		}
		bucket.Name = bucketName
		return bucket, nil
	}
	unmarshaller := func(in []byte) (interface{}, error) {
		var bucket Bucket
		err := json.Unmarshal(in, &bucket)
		return bucket, err
	}
	b, err := m.Cache.Get(redis.BucketTable, bucketName, getBucket, unmarshaller)
	if err != nil {
		return
	}
	bucket, ok := b.(Bucket)
	if !ok {
		helper.Debugln("Cast b failed:", b)
		err = ErrInternalError
		return
	}
	return bucket, nil
}

func (m *Meta) UpdateUsage(bucketName string, size int64) {
	inc, err := hrpc.NewIncStrSingle(context.Background(), BUCKET_TABLE,
		bucketName, BUCKET_COLUMN_FAMILY, "usage", size)
	retValue, err := m.Hbase.Increment(inc)
	if err != nil {
		helper.Logger.Println("Inconsistent data: usage of bucket", bucketName,
			"should add by", size)
	}
	m.Cache.Remove(redis.BucketTable, bucketName)
	helper.Debugln("New usage:", retValue)
}

func (m *Meta) GetUsage(bucketName string) (int64, error) {
	bucket, err := m.GetBucket(bucketName)
	if err != nil {
		return 0, err
	}
	return bucket.Usage, nil
}
