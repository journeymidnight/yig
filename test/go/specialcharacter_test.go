package _go

import (
	"encoding/xml"
	"github.com/journeymidnight/yig/api/datatype"
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

// This test case is used to test whether the result of obtaining an Object by an external user is correct
// before setting the public-read ACL and setting the public-read ACL.
func Test_PutSpecialCharaterObjectPublicAclWithXml(t *testing.T) {
	sc := NewS3()
	url := GenTestSpecialCharaterObjectUrl(sc)

	// before set public-read ACL.
	statusCode, _, err := HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be AccessDenied
	if statusCode != http.StatusForbidden {
		t.Fatal("StatusCode should be AccessDenied(403), but the code is:", statusCode)
	}
	t.Log("GetObject Without public-read ACL test Success.")

	// set public-read ACL.
	var policy = &datatype.AccessControlPolicy{}
	err = xml.Unmarshal([]byte(AclPublicXml), policy)
	if err != nil {
		t.Fatal("PutObjectPublicAclWithXml err:", err)
	}
	acl := TransferToS3AccessControlPolicy(policy)
	if acl == nil {
		t.Fatal("PutObjectPublicAclWithXml err:", "empty acl!")
	}
	err = sc.PutObjectAclWithXml(TEST_BUCKET, TEST_KEY_SPECIAL, acl)
	if err != nil {
		t.Fatal("PutObjectAclWithXml err:", err)
	}
	t.Log("PutObjectAclWithXml Success!")

	out, err := sc.GetObjectAcl(TEST_BUCKET, TEST_KEY_SPECIAL)
	if err != nil {
		t.Fatal("GetObjectAcl err:", err)
	}
	t.Log("GetObjectAcl Success! out:", out)

	// After set public-read ACL.
	statusCode, data, err := HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be STATUS_OK
	if statusCode != http.StatusOK {
		t.Fatal("StatusCode should be STATUS_OK(200), but the code is:", statusCode)
	}
	t.Log("Get object value:", string(data))
}

// This test case is used to test whether the result of obtaining an Object by an external user is correct
// before setting the private ACL and setting the private ACL.
func Test_PutSpecialCharaterObjectPrivateAclWithXml(t *testing.T) {
	sc := NewS3()
	url := GenTestSpecialCharaterObjectUrl(sc)

	// before set private ACL.
	statusCode, data, err := HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be STATUS_OK
	if statusCode != http.StatusOK {
		t.Fatal("StatusCode should be STATUS_OK(200), but the code is:", statusCode)
	}
	t.Log("Get object value:", string(data))

	//set private ACL.
	var policy = &datatype.AccessControlPolicy{}
	err = xml.Unmarshal([]byte(AclPrivateXml), policy)
	if err != nil {
		t.Fatal("PutObjectPrivateAclWithXml err:", err)
	}
	acl := TransferToS3AccessControlPolicy(policy)
	if acl == nil {
		t.Fatal("PutObjectPrivateAclWithXml err:", "empty acl!")
	}
	err = sc.PutObjectAclWithXml(TEST_BUCKET, TEST_KEY_SPECIAL, acl)
	if err != nil {
		t.Fatal("PutObjectAclWithXml err:", err)
	}
	t.Log("PutObjectAclWithXml Success!")

	out, err := sc.GetObjectAcl(TEST_BUCKET, TEST_KEY_SPECIAL)
	if err != nil {
		t.Fatal("GetObjectAcl err:", err)
	}
	t.Log("GetObjectAcl Success! out:", out)

	// After set private ACL.
	statusCode, _, err = HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be AccessDenied
	if statusCode != http.StatusForbidden {
		t.Fatal("StatusCode should be AccessDenied(403), but the code is:", statusCode)
	}
	t.Log("GetObject With private ACL test Success.")
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