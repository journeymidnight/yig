package _go

import (
	. "github.com/journeymidnight/yig/test/go/lib"
	"testing"
)

func Test_BucketWebSite(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	err = sc.PutBucketWebsite(TEST_BUCKET, "index.html", "error.html")
	if err != nil {
		t.Fatal("PutBucketWebsite err:", err)
		panic(err)
	}
	out, err := sc.GetBucketWebsite(TEST_BUCKET)
	t.Log("Webstite:", out)

	err = sc.DeleteBucketWebsite(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucketWebsite err:", err)
		panic(err)
	}
	sc.DeleteBucket(TEST_BUCKET)

}
