package _go

import (
	. "github.com/journeymidnight/yig/test/go/lib"
	"net/http"
	"testing"
)

func Test_SpecialCharaterObject_Prepare(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
}

func Test_PutSpecialCharacterObject(t *testing.T) {
	sc := NewS3()
	err := sc.PutObject(TEST_BUCKET, TEST_KEY_SPECIAL, TEST_VALUE)
	if err != nil {
		t.Fatal("PutSpecialCharacterObject err:", err)
	}
	t.Log("PutSpecialCharacterObject Success!")
}

func Test_GetSpecialCharacterObject(t *testing.T) {
	sc := NewS3()
	v, err := sc.GetObject(TEST_BUCKET, TEST_KEY_SPECIAL)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	if v != TEST_VALUE {
		t.Fatal("GetSpecialCharacterObject err: value is:", v, ", but should be:", TEST_VALUE)
	}
	t.Log("GetSpecialCharacterObject Success value:", v)
}

func Test_PutSpecialCharacterObjectAcl(t *testing.T) {
	sc := NewS3()
	err := sc.PutObjectAcl(TEST_BUCKET, TEST_KEY_SPECIAL, BucketCannedACLPublicRead)
	if err != nil {
		t.Fatal("PutObjectAcl err:", err)
	}
	t.Log("PutObjectAcl Success!")
}

func Test_GetSpecialCharaterObjectPublicAcl(t *testing.T) {
	sc := NewS3()
	url := GenTestSpecialCharaterObjectUrl(sc)
	statusCode, _, err := HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be AccessDenied
	if statusCode != http.StatusOK {
		t.Fatal("StatusCode should be STATUS_OK(200), but the code is:", statusCode)
	}
	t.Log("GetSpecialCharacterObject With public-read ACL test Success.")
}

func Test_SpecialCharaterObject_End(t *testing.T) {
	sc := NewS3()
	err := sc.DeleteObject(TEST_BUCKET, TEST_KEY_SPECIAL)
	if err != nil {
		t.Log("DeleteSpecialCharacterObject err:", err)
	}
	err = sc.DeleteBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}

}