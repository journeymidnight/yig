package _go

import (
	"testing"

	. "github.com/journeymidnight/yig/test/go/lib"
)

func Test_Bucket(t *testing.T) {
	sc := NewS3()
	defer func() {
		sc.DeleteBucket(TestBucket)
		sc.DeleteBucket(TestBucket + "2")
	}()
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")

	err = sc.MakeBucket(TestBucket + "2")
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	err = sc.HeadBucket(TestBucket)
	if err != nil {
		t.Fatal("HeadBucket err:", err)
	}
	t.Log("HeadBucket Success.")

	buckets, err := sc.ListBuckets()
	if err != nil {
		t.Fatal("ListBuckets err:", err)
	}

	if !HasStrInSlice(buckets, TestBucket) && !HasStrInSlice(buckets, TestBucket+"2") {
		t.Fatal("Buckets' name is wrong", buckets)
	}
	t.Log("ListBucket Success.")
}
