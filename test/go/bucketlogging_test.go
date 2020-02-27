package _go
import (
	. "github.com/journeymidnight/yig/test/go/lib"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"testing"
)

func Test_BucketLogging_Prepare(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")
}

func Test_PutBucketLogging(t *testing.T) {
	sc := NewS3()
	rules := &s3.LoggingEnabled{
		TargetBucket: aws.String("testTargetBucket"),
		TargetPrefix: aws.String("testTargetPrefix"),
	}
	err := sc.PutBucketLogging(TEST_BUCKET, rules)
	if err != nil {
		t.Fatal("PutBucketLogging err:", err)
		panic(err)
	}
	t.Log("PutBucketLogging Success.")
}

func Test_GetBucketLogging(t *testing.T) {
	sc := NewS3()
	err := sc.GetBucketLogging(TEST_BUCKET)
	if err != nil {
		t.Fatal("GetBucketLogging err:", err)
		panic(err)
	}
	t.Log("GetBucketLogging Success.")
}

func Test_DeleteBucketLogging(t *testing.T) {
	sc := NewS3()
	rules := &s3.LoggingEnabled{
		TargetBucket: aws.String(""),
		TargetPrefix: aws.String(""),
	}
	err := sc.PutBucketLogging(TEST_BUCKET, rules)
	if err != nil {
		t.Fatal("DeleteBucketLogging err:", err)
		panic(err)
	}
	t.Log("DeleteBucketLogging Success.")
}

