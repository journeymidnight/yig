package _go

import (
	"testing"

	. "github.com/journeymidnight/yig/test/go/lib"
)

func Test_MultiDomain(t *testing.T) {
	sc := NewS3Internal()
	defer sc.CleanEnv()
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")

	err = sc.PutObject(TestBucket, TestKey, TestValue)
	if err != nil {
		t.Fatal("PutObject err:", err)
		panic(err)
	}
	t.Log("PutObject Success.")

	err = sc.DeleteObject(TestBucket, TestKey)
	if err != nil {
		t.Fatal("DeleteObject err:", err)
	}

	err = sc.DeleteBucket(TestBucket)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
}
