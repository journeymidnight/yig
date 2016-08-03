package meta

import (
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
)

type Bucket struct {
	Name string
	// Date and time when the bucket was created,
	// in format "2006-01-02T15:04:05.000Z"
	CreateTime string
	OwnerId    string
	CORS       string
	ACL        string
}

func (m *Meta) GetBucketInfo(bucketName string) (bucket Bucket, err error) {
	getRequest, err := hrpc.NewGetStr(context.Background(), BUCKET_TABLE, bucketName)
	if err != nil {
		return
	}
	response, err := m.Hbase.Get(getRequest)
	if err != nil {
		return
	}
	if len(response.Cells) == 0 {
		err = BucketNotFound{Bucket: bucketName}
		return
	}
	for _, cell := range response.Cells {
		switch string(cell.Qualifier) {
		case "createTime":
			bucket.CreateTime = string(cell.Value)
		case "UID":
			bucket.OwnerId = string(cell.Value)
		case "CORS":
			bucket.CORS = string(cell.Value)
		case "ACL":
			bucket.ACL = string(cell.Value)
		default:
		}
	}
	bucket.Name = bucketName
	return
}
