package _go

import (
	. "github.com/journeymidnight/yig/test/go/lib"
	"testing"
)

func Test_Encrypt_Prepare(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
}

func Test_PutEncryptObjectWithSSEC(t *testing.T) {
	sc := NewS3()
	err := sc.PutEncryptObjectWithSSEC(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutEncryptObjectWithSSEC err:", err)
	}
	t.Log("PutEncryptObjectWithSSEC Success!")
}

func TestS3Client_GetEncryptObjectWithSSEC(t *testing.T) {
	sc := NewS3()
	v, err := sc.GetEncryptObjectWithSSEC(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("GetEncryptObjectWithSSEC err:", err)
	}
	if v != TEST_VALUE {
		t.Fatal("GetEncryptObjectWithSSEC err: value is:", v, ", but should be:", TEST_VALUE)
	}
	t.Log("GetEncryptObjectWithSSEC Success value:", v)
}

func Test_Encrypt_End(t *testing.T) {
	sc := NewS3()
	err := sc.DeleteObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Log("DeleteObject err:", err)
	}
	err = sc.DeleteBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
}
