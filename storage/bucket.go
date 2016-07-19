package storage

import (
	"git.letv.cn/yig/yig/iam"
	"git.letv.cn/yig/yig/minio/datatype"
	"github.com/tsuna/gohbase/hrpc"
	"errors"
	"golang.org/x/net/context"
)

func (yig *YigStorage) MakeBucket(bucket string, credential iam.Credential) error {
	values := map[string]map[string][]byte{
		BUCKET_COLUMN_FAMILY: map[string][]byte{
			"CORS": []byte{}, // TODO
			"UID":  []byte(credential.UserId),
			"ACL":  []byte{}, // TODO
		},
	}
	put, err := hrpc.NewPutStr(context.Background(), BUCKET_TABLE, bucket, values)
	if err != nil {
		yig.Logger.Println("Error making hbase put: ", err)
		return errors.New("Make bucket error")
	}
	processed, err := yig.Hbase.CheckAndPut(put, BUCKET_COLUMN_FAMILY, "UID", []byte{})
	if err != nil {
		yig.Logger.Println("Error checkandput: ", err)
		return errors.New("Make bucket error")
	}
	if !processed {
		return errors.New("Bucket already exists")
	}
	// TODO: update users table
	return nil
}

func (yig *YigStorage) GetBucketInfo(bucket string) (bucketInfo datatype.BucketInfo, err error) {
	return
}

func (yig *YigStorage) ListBuckets() (buckets []datatype.BucketInfo, err error) {
	return
}

func (yig *YigStorage) DeleteBucket(bucket string) error {
	return nil
}

func (yig *YigStorage) ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result datatype.ListObjectsInfo, err error) {
	return
}
