package _go

import (
	"net/http"
	"os"
	"strings"
	"testing"
)

var (
	GetObjectPolicy_1 = `{
			"Version": "2012-10-17",
			"Statement": [{
			"Effect": "Allow",
			"Principal": {"AWS":["*"]},
			"Action": ["s3:GetObject"],
			"Resource": [
				"arn:aws:s3:::` + TEST_BUCKET + `/*"
			]
			}]
		}`

	GetObjectPolicy_2 = `{
			"Version": "2012-10-17",
			"Statement": [{
			"Effect": "Allow",
			"Principal": {"AWS":["*"]},
			"Action": ["s3:GetObject"],
			"Resource": [
				"arn:aws:s3:::` + TEST_BUCKET + `/test/*"
			]
			}]
		}`
)

func Test_Bucket_Prepare(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
}

func Test_PutBucketPolicy(t *testing.T) {
	sc := NewS3()
	err := sc.PutBucketPolicy(TEST_BUCKET, GetObjectPolicy_1)
	if err != nil {
		t.Fatal("PutBucketPolicy err:", err)
	}
	t.Log("PutBucketPolicy success.")

}

func Test_GetBucketPolicy(t *testing.T) {
	sc := NewS3()
	policy, err := sc.GetBucketPolicy(TEST_BUCKET)
	if err != nil {
		t.Fatal("GetBucketPolicy err:", err)
	}
	p_str := format(policy)
	origin_p_str := format(GetObjectPolicy_1)

	if p_str != origin_p_str {
		t.Fatal("GetBucketPolicy is not correct! origin:", origin_p_str, "policy:", p_str)
	}
	t.Log("GetBucketPolicy success.")
}

func Test_DeleteBucketPolicy(t *testing.T) {
	sc := NewS3()
	err := sc.DeleteBucketPolicy(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucketPolicy err:", err)
	}
	policy, err := sc.GetBucketPolicy(TEST_BUCKET)
	if err != nil {
		t.Fatal("GetBucketPolicy err:", err)
	}

	p_str := format(policy)
	origin_p_str := format(GetObjectPolicy_1)

	if p_str == origin_p_str {
		t.Fatal("DeleteBucketPolicy not success:", policy)
	}

	t.Log("DeleteBucketPolicy success.")

}

func Test_BucketPolicySample_1(t *testing.T) {
	sc := NewS3()
	err := sc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}

	//Anonymous to get
	url := "http://" + *sc.Client.Config.Endpoint + string(os.PathSeparator) + TEST_BUCKET + string(os.PathSeparator) + TEST_KEY

	statusCode, _, err := HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be AccessDenied
	if statusCode != http.StatusForbidden {
		t.Fatal("StatusCode should be AccessDenied(403), but the code is:", statusCode)
	}

	err = sc.PutBucketPolicy(TEST_BUCKET, GetObjectPolicy_1)
	if err != nil {
		t.Fatal("PutBucketPolicy err:", err)
	}

	policy, err := sc.GetBucketPolicy(TEST_BUCKET)
	if err != nil {
		t.Fatal("GetBucketPolicy err:", err)
	}
	t.Log("Bucket policy:", format(policy))

	// After set policy
	statusCode, data, err := HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be STATUS_OK
	if statusCode != http.StatusOK {
		t.Fatal("StatusCode should be STATUS_OK(200), but the code is:", statusCode)
	}
	t.Log("Get object value:", string(data))

	err = sc.DeleteBucketPolicy(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucketPolicy err:", err)
	}

	//After delete policy
	statusCode, _, err = HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be AccessDenied
	if statusCode != http.StatusForbidden {
		t.Fatal("StatusCode should be AccessDenied(403), but the code is:", statusCode)
	}

	err = sc.DeleteObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("DeleteObject err:", err)
	}
}

func Test_Bucket_End(t *testing.T) {
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

func format(s string) string {
	return strings.Replace(strings.Replace(strings.Replace(s, " ", "", -1), "\n", "", -1), "\t", "", -1)
}
