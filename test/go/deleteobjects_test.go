package _go

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	. "github.com/journeymidnight/yig/test/go/lib"
)

func Test_DeleteObjects(t *testing.T) {
	sc := NewS3()
	defer func() {
		sc.DeleteObject(TEST_BUCKET, TEST_KEY)
		sc.DeleteObject(TEST_BUCKET, TEST_KEY_SPECIAL)
		sc.DeleteBucket(TEST_BUCKET)
	}()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	err = sc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
		panic(err)
	}

	err = sc.PutObject(TEST_BUCKET, TEST_KEY_SPECIAL, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
		panic(err)
	}

	var objects []*s3.ObjectIdentifier
	objects = append(objects, &s3.ObjectIdentifier{Key: aws.String(TEST_KEY)}, &s3.ObjectIdentifier{Key: aws.String(TEST_KEY_SPECIAL)})
	testDelete := &s3.Delete{Objects: objects}
	result, err := sc.DeleteObjects(TEST_BUCKET, testDelete)
	if err != nil {
		t.Fatal("DeleteObjects err:", err)
	}
	if len(result.Deleted) != 2 {
		t.Fatal("DeleteObjects failed.")
	}
	for _, obj := range result.Deleted {
		t.Log("Delete:", obj.Key, obj.VersionId, obj.DeleteMarker, obj.DeleteMarkerVersionId)
	}
	for _, err := range result.Errors {
		t.Log("DeleteError:", err.Key, err.VersionId, err.Code, err.Message)
	}
}
