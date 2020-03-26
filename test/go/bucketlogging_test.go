package _go

import (
	"testing"

	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	. "github.com/journeymidnight/yig/test/go/lib"
)

func Test_BucketLogging(t *testing.T) {
	sc := NewS3()
	defer func() {
		sc.DeleteBucket(TEST_BUCKET)
	}()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")

	rules := &s3.LoggingEnabled{
		TargetBucket: aws.String("testTargetBucket"),
		TargetPrefix: aws.String("testTargetPrefix"),
	}
	err = sc.PutBucketLogging(TEST_BUCKET, rules)
	if err != nil {
		t.Fatal("PutBucketLogging err:", err)
		panic(err)
	}
	t.Log("PutBucketLogging Success.")

	out, err := sc.GetBucketLogging(TEST_BUCKET)
	if err != nil {
		t.Fatal("GetBucketLogging err:", err)
		panic(err)
	}
	t.Log("GetBucketAcl Success! out:", out)

	rules = &s3.LoggingEnabled{
		TargetBucket: aws.String(""),
		TargetPrefix: aws.String(""),
	}
	err = sc.PutBucketLogging(TEST_BUCKET, rules)
	if err != nil {
		t.Fatal("DeleteBucketLogging err:", err)
		panic(err)
	}
	t.Log("DeleteBucketLogging Success.")

	err = sc.DeleteBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
}
