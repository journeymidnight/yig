package meta

import (
	"encoding/json"
	"git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
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
}

func (b Bucket) GetValues() (values map[string]map[string][]byte, err error) {
	cors, err := json.Marshal(b.CORS)
	if err != nil {
		return
	}
	values = map[string]map[string][]byte{
		BUCKET_COLUMN_FAMILY: map[string][]byte{
			"UID":        []byte(b.OwnerId),
			"ACL":        []byte(b.ACL.CannedAcl),
			"CORS":       cors,
			"createTime": []byte(b.CreateTime.Format(CREATE_TIME_LAYOUT)),
		},
		// TODO fancy ACL
	}
	return
}

// TODO: make this query more fine-grained to cell level
// otherwise there might be race-conditions, e.g. between SetBucketAcl and SetBucketCors
func (m *Meta) GetBucket(bucketName string) (bucket Bucket, err error) {
	getRequest, err := hrpc.NewGetStr(context.Background(), BUCKET_TABLE, bucketName)
	if err != nil {
		return
	}
	response, err := m.Hbase.Get(getRequest)
	if err != nil {
		return
	}
	if len(response.Cells) == 0 {
		err = ErrNoSuchBucket
		return
	}
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
			err = json.Unmarshal(cell.Value, cors)
			if err != nil {
				return
			}
			bucket.CORS = cors
		case "ACL":
			bucket.ACL.CannedAcl = string(cell.Value)
		default:
		}
	}
	bucket.Name = bucketName
	return
}
