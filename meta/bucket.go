package meta

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"legitlab.letv.cn/yig/yig/api/datatype"
	. "legitlab.letv.cn/yig/yig/error"
	"legitlab.letv.cn/yig/yig/helper"
	"legitlab.letv.cn/yig/yig/redis"
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
	LC	   datatype.Lc
	Versioning string // actually enum: Disabled/Enabled/Suspended
	Usage      int64
}

func (b Bucket) GetValues() (values map[string]map[string][]byte, err error) {
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
	values = map[string]map[string][]byte{
		BUCKET_COLUMN_FAMILY: map[string][]byte{
			"UID":        []byte(b.OwnerId),
			"ACL":        []byte(b.ACL.CannedAcl),
			"CORS":       cors,
			"LC":         lc,
			"createTime": []byte(b.CreateTime.Format(CREATE_TIME_LAYOUT)),
			"versioning": []byte(b.Versioning),
			"usage":      usage.Bytes(),
		},
		// TODO fancy ACL
	}
	return
}

// Note the usage info got from this method is possibly not accurate because we don't
// invalid cache when updating usage. For accurate usage info, use `GetUsage()`
func (m *Meta) GetBucket(bucketName string, willNeed bool) (bucket Bucket, err error) {
	getBucket := func() (b interface{}, err error) {
		ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
		defer done()
		getRequest, err := hrpc.NewGetStr(ctx, BUCKET_TABLE, bucketName)
		if err != nil {
			return
		}
		response, err := m.Hbase.Get(getRequest)
		if err != nil {
			m.Logger.Println(5, "Error getting bucket info, with error ", err)
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
			case "LC":
				var lc datatype.Lc
				err = json.Unmarshal(cell.Value, &lc)
				if err != nil {
					return
				}
				bucket.LC = lc
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
	b, err := m.Cache.Get(redis.BucketTable, bucketName, getBucket, unmarshaller, willNeed)
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
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	inc, err := hrpc.NewIncStrSingle(ctx, BUCKET_TABLE, bucketName,
		BUCKET_COLUMN_FAMILY, "usage", size)
	retValue, err := m.Hbase.Increment(inc)
	if err != nil {
		helper.Logger.Println(5, "Inconsistent data: usage of bucket", bucketName,
			"should add by", size)
	}
	helper.Debugln("New usage:", retValue)
}

func (m *Meta) GetUsage(bucketName string) (int64, error) {
	m.Cache.Remove(redis.BucketTable, bucketName)
	bucket, err := m.GetBucket(bucketName, true)
	if err != nil {
		return 0, err
	}
	return bucket.Usage, nil
}

func (m *Meta) GetBucketInfo(bucketName string) (Bucket, error) {
	m.Cache.Remove(redis.BucketTable, bucketName)
	bucket, err := m.GetBucket(bucketName, true)
	if err != nil {
		return bucket, err
	}
	return bucket, nil
}

func (m *Meta) GetUserInfo(uid string) ([]string, error) {
	m.Cache.Remove(redis.UserTable, uid)
	buckets, err := m.GetUserBuckets(uid, true)
	if err != nil {
		return nil, err
	}
	return buckets, nil
}
