package _go

import (
	"testing"

	. "github.com/journeymidnight/yig/test/go/lib"
)

func Test_Bucket(t *testing.T) {
	sc := NewS3()
	defer func() {
		sc.DeleteBucket(TEST_BUCKET)
		sc.DeleteBucket(TEST_BUCKET + "2")
	}()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")

	err = sc.MakeBucket(TEST_BUCKET + "2")
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	err = sc.HeadBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("HeadBucket err:", err)
	}
	t.Log("HeadBucket Success.")

	buckets, err := sc.ListBuckets()
	if err != nil {
		t.Fatal("ListBuckets err:", err)
	}

	if !HasStrInSlice(buckets, TEST_BUCKET) && !HasStrInSlice(buckets, TEST_BUCKET+"2") {
		t.Fatal("Buckets' name is wrong", buckets)
	}
	t.Log("ListBucket Success.")
}
