package _go

import (
	"net/http"
	"testing"

	. "github.com/journeymidnight/yig/test/go/lib"
)

func Test_SpecialCharaterObject(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	err = sc.PutObject(TestBucket, TestKeySpecial, TestValue)
	if err != nil {
		t.Fatal("PutSpecialCharacterObject err:", err)
	}
	t.Log("PutSpecialCharacterObject Success!")
	v, err := sc.GetObject(TestBucket, TestKeySpecial)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	if v != TestValue {
		t.Fatal("GetSpecialCharacterObject err: value is:", v, ", but should be:", TestValue)
	}
	t.Log("GetSpecialCharacterObject Success value:", v)
	err = sc.PutObjectAcl(TestBucket, TestKeySpecial, BucketCannedACLPublicRead)
	if err != nil {
		t.Fatal("PutObjectAcl err:", err)
	}
	t.Log("PutObjectAcl Success!")
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
	err = sc.DeleteObject(TestBucket, TestKeySpecial)
	if err != nil {
		t.Log("DeleteSpecialCharacterObject err:", err)
	}
	err = sc.DeleteBucket(TestBucket)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
}
