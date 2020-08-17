package _go

import (
	"testing"
	"time"

	. "github.com/journeymidnight/yig/test/go/lib"
)

func Test_Migrate(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	delFn := func(sc *S3Client) {
		sc.DeleteObject(TestBucket, TestKey)
		sc.DeleteBucket(TestBucket)
	}
	defer delFn(sc)
	var nextPos int64
	nextPos, err = sc.AppendObject(TestBucket, TestKey, TestValue, nextPos)
	if err != nil {
		t.Fatal("AppendObject err:", err)
	}
	t.Log("First AppendObject Success! Next position:", nextPos)
	v, err := sc.GetObject(TestBucket, TestKey)
	t.Log("First Append Value:", v)
	if v != TestValue {
		t.Fatal("GetObject err: value is:", v, ", but should be:", TestValue)
	}
	// wait for migrate
	t.Log("Sleep for 15second:", nextPos)
	time.Sleep(15 * time.Second)
	//still can get data
	v, err = sc.GetObject(TestBucket, TestKey)
	t.Log("First Append Value:", v)
	if v != TestValue {
		t.Fatal("GetObject err: value is:", v, ", but should be:", TestValue)
	}

	nextPos, err = sc.AppendObject(TestBucket, TestKey, TestValue+"APPEND", nextPos)
	if err != nil {
		t.Fatal("AppendObject err:", err)
	}
	v, err = sc.GetObject(TestBucket, TestKey)
	if v != TestValue+TestValue+"APPEND" {
		t.Fatal("GetObject err: value is:", v, ", but should be:", TestValue+TestValue+"APPEND")
	}
	t.Log("AppendObject success!")

}
