package _go

import (
	"testing"
	"time"

	. "github.com/journeymidnight/yig/test/go/lib"
)

func Test_Migrate(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	delFn := func(sc *S3Client) {
		sc.DeleteObject(TEST_BUCKET, TEST_KEY)
		sc.DeleteBucket(TEST_BUCKET)
	}
	defer delFn(sc)
	var nextPos int64
	nextPos, err = sc.AppendObject(TEST_BUCKET, TEST_KEY, TEST_VALUE, nextPos)
	if err != nil {
		t.Fatal("AppendObject err:", err)
	}
	t.Log("First AppendObject Success! Next position:", nextPos)
	v, err := sc.GetObject(TEST_BUCKET, TEST_KEY)
	t.Log("First Append Value:", v)
	if v != TEST_VALUE {
		t.Fatal("GetObject err: value is:", v, ", but should be:", TEST_VALUE)
	}
	// wait for migrate
	t.Log("Sleep for 15second:", nextPos)
	time.Sleep(15 * time.Second)
	//still can get data
	v, err = sc.GetObject(TEST_BUCKET, TEST_KEY)
	t.Log("First Append Value:", v)
	if v != TEST_VALUE {
		t.Fatal("GetObject err: value is:", v, ", but should be:", TEST_VALUE)
	}

	nextPos, err = sc.AppendObject(TEST_BUCKET, TEST_KEY, TEST_VALUE+"APPEND", nextPos)
	if err != nil {
		t.Fatal("AppendObject err:", err)
	}
	v, err = sc.GetObject(TEST_BUCKET, TEST_KEY)
	if v != TEST_VALUE+TEST_VALUE+"APPEND" {
		t.Fatal("GetObject err: value is:", v, ", but should be:", TEST_VALUE+TEST_VALUE+"APPEND")
	}
	t.Log("AppendObject success!")

}
