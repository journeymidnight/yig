package _go

import "testing"

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

func Test_PutEncryptObjectWithSSES3(t *testing.T) {
	sc := NewS3()
	err := sc.PutEncryptObjectWithSSES3(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutEncryptObjectWithSSES3 err:", err)
	}
	t.Log("PutEncryptObjectWithSSES3 Success!")
}

func TestS3Client_GetEncryptObjectWithSSES3(t *testing.T) {
	sc := NewS3()
	v, err := sc.GetEncryptObjectWithSSES3(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("GetEncryptObjectWithSSES3 err:", err)
	}
	if v != TEST_VALUE {
		t.Fatal("GetEncryptObjectWithSSES3 err: value is:", v, ", but should be:", TEST_VALUE)
	}
	t.Log("GetEncryptObjectWithSSES3 Success value:", v)
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
