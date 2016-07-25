package meta

import (
	"encoding/json"
	"errors"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
	"git.letv.cn/yig/yig/minio/datatype"
)

func (m *Meta) ensureUserExists(userId string) error {
	emptyArray, err := json.Marshal([]string{})
	if err != nil {
		return err
	}
	emptyUser := map[string]map[string][]byte{
		USER_COLUMN_FAMILY: map[string][]byte{
			"buckets": emptyArray,
		},
	}
	put, err := hrpc.NewPutStr(context.Background(), USER_TABLE, userId, emptyUser)
	if err != nil {
		return err
	}
	_, err = m.Hbase.CheckAndPut(put, USER_COLUMN_FAMILY, "buckets", []byte{})
	return err
}

func (m *Meta) GetUserBuckets(userId string) (buckets []string, err error) {
	family := hrpc.Families(map[string][]string{USER_COLUMN_FAMILY: []string{"buckets"}})
	getRequest, err := hrpc.NewGetStr(context.Background(), USER_TABLE, userId, family)
	if err != nil {
		return
	}
	userBuckets, err := m.Hbase.Get(getRequest)
	if err != nil {
		m.Logger.Println("Error getting user info, with error ", err)
		return
	}
	err = json.Unmarshal(userBuckets.Cells[0].Value, &buckets)
	if err != nil {
		m.Logger.Println("Error unmarshalling user buckets for ", userId,
			"with error ", err)
		return
	}
	return buckets, nil
}

func (m *Meta) GetBucketInfo(bucketName string) (bucket Bucket, err error) {
	getRequest, err := hrpc.NewGetStr(context.Background(), BUCKET_TABLE, bucket)
	if err != nil {
		return
	}
	response, err := m.Hbase.Get(getRequest)
	if err != nil {
		return
	}
	if len(response.Cells) == 0 {
		err = datatype.BucketNotFound{Bucket: bucket}
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

func (m *Meta) AddBucketForUser(bucket string, userId string) error {
	err := m.ensureUserExists(userId)
	if err != nil {
		return err
	}
	family := hrpc.Families(map[string][]string{USER_COLUMN_FAMILY: []string{"buckets"}})
	getRequest, err := hrpc.NewGetStr(context.Background(), USER_TABLE, userId, family)
	if err != nil {
		return err
	}
	tries := 0
	for tries < RETRY_LIMIT {
		tries += 1
		currentUser, err := m.Hbase.Get(getRequest)
		if err != nil {
			m.Logger.Println("Error getting user info, with error ", err)
			continue
		}
		var currentBuckets []string
		err = json.Unmarshal(currentUser.Cells[0].Value, &currentBuckets)
		if err != nil {
			m.Logger.Println("Error unmarshalling user buckets for ", userId,
				"with error ", err)
			continue
		}
		// TODO check user bucket number limit

		newBuckets := append(currentBuckets, bucket)
		newBucketsMarshaled, err := json.Marshal(newBuckets)
		if err != nil {
			m.Logger.Println("Error marshalling json: ", err)
			continue
		}
		newUser := map[string]map[string][]byte{
			USER_COLUMN_FAMILY: map[string][]byte{
				"buckets": newBucketsMarshaled,
			},
		}
		newUserPut, err := hrpc.NewPutStr(context.Background(), USER_TABLE, userId, newUser)
		if err != nil {
			m.Logger.Println("Error making hbase put: ", err)
			continue
		}
		processed, err := m.Hbase.CheckAndPut(newUserPut, USER_COLUMN_FAMILY,
			"buckets", currentUser.Cells[0].Value)
		if err != nil {
			m.Logger.Println("Error CheckAndPut: ", err)
			continue
		}
		if processed {
			return nil
		}
	}
	return errors.New("Cannot add bucket " + bucket + " for user " + userId)
}
