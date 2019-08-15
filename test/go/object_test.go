package _go

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	. "github.com/journeymidnight/yig/test/go/lib"
)

func Test_Object_Prepare(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
}

func Test_PutObject(t *testing.T) {
	sc := NewS3()
	err := sc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	t.Log("PutObject Success!")
}

func Test_HeadObject(t *testing.T) {
	sc := NewS3()
	err := sc.HeadObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("HeadBucket err:", err)
	}
	t.Log("HeadObject Success!")
}

func Test_GetObject(t *testing.T) {
	sc := NewS3()
	v, err := sc.GetObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	if v != TEST_VALUE {
		t.Fatal("GetObject err: value is:", v, ", but should be:", TEST_VALUE)
	}
	t.Log("GetObject Success value:", v)
}

func Test_DeleteObject(t *testing.T) {
	sc := NewS3()
	err := sc.DeleteObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("DeleteObject err:", err)
	}
	err = sc.HeadObject(TEST_BUCKET, TEST_KEY)
	if err == nil {
		t.Fatal("HeadObject err:", err)
	}
	t.Log("DeleteObject Success!")
}

func Test_PostObject(t *testing.T) {
	pbi := &PostObjectInput{
		Url:        fmt.Sprintf("http://s3.test.com:8080/%s", TEST_BUCKET),
		Bucket:     TEST_BUCKET,
		ObjName:    TEST_KEY,
		Expiration: time.Now().UTC().Add(time.Duration(1 * time.Hour)),
		Date:       time.Now().UTC(),
		Region:     "r",
		AK:         "hehehehe",
		SK:         "hehehehe",
		FileSize:   1024,
	}

	sc := NewS3()
	sc.MakeBucket(TEST_BUCKET)
	err := sc.PostObject(pbi)
	if err != nil {
		t.Fatal("PostObject err:", err)
	}
	t.Log("PostObject Success!")
}
func Test_PreSignedGetObject(t *testing.T) {
	sc := NewS3()
	err := sc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	url, err := sc.GetObjectPreSigned(TEST_BUCKET, TEST_KEY, 5*time.Second)
	if err != nil {
		t.Fatal("GetObjectPreSigned err:", err)
	}
	t.Log("url:", url)
	// After set presign
	statusCode, data, err := HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be STATUS_OK
	if statusCode != http.StatusOK {
		t.Fatal("StatusCode should be STATUS_OK(200), but the code is:", statusCode)
	}
	t.Log("Get object value:", string(data))

	//After 5 second
	time.Sleep(5 * time.Second)
	statusCode, _, err = HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be AccessDenied
	if statusCode != http.StatusForbidden {
		t.Fatal("StatusCode should be AccessDenied(403), but the code is:", statusCode)
	}
	t.Log("PreSignedGetObject Success.")
}

func Test_CopyObject(t *testing.T) {
	//non-cryption
	svc := NewS3()
	err := svc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}

	TEST_COPY_KEY := "COPYED:" + TEST_KEY
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(TEST_BUCKET),
		CopySource: aws.String(TEST_BUCKET + "/" + TEST_KEY),
		Key:        aws.String(TEST_COPY_KEY),
	}
	_, err = svc.Client.CopyObject(input)
	if err != nil {
		t.Fatal("Copy Object err:", err)
	}

	//verify them
	v1, err := svc.GetObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	v2, err := svc.GetObject(TEST_BUCKET, TEST_COPY_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	if v1 != v2 {
		t.Fatal("Copyed result is not the same.")
	}

	//clean up
	svc.DeleteObject(TEST_BUCKET, TEST_KEY)
	svc.DeleteObject(TEST_BUCKET, TEST_COPY_KEY)
}

func Test_CopyObjectWithReplace(t *testing.T) {
	svc := NewS3()
	err := svc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}

	TEST_COPY_KEY := "COPYED:" + TEST_KEY
	input := &s3.CopyObjectInput{
		Bucket:            aws.String(TEST_BUCKET),
		CopySource:        aws.String(TEST_BUCKET + "/" + TEST_KEY),
		Key:               aws.String(TEST_COPY_KEY),
		MetadataDirective: aws.String("REPLACE"),
		CacheControl:      aws.String("max-age:1983"),
		ContentType:       aws.String("image/jpeg"),
		Metadata: map[string]*string{
			"merry":     aws.String("christmas"), //in
			"happy":     aws.String("new year"),  //in
			"Christmas": aws.String("EVE"),       //in
			"hello":     aws.String("world"),     //out
		},
	}

	_, err = svc.Client.CopyObject(input)
	if err != nil {
		t.Fatal("Copy Object err:", err)
	}
	// check the connn
	params := &s3.HeadObjectInput{
		Bucket: aws.String(TEST_BUCKET),
		Key:    aws.String(TEST_COPY_KEY),
	}

	headResult, err := svc.Client.HeadObject(params)
	if err != nil {
		t.Fatal("Head object failed")
	}

	if *headResult.ContentType != "image/jpeg" {
		t.Fatal("failed to set content type")
	}

	for k, v := range headResult.Metadata {
		switch k {
		case "merry":
			if *v != "christmas" {
				t.Fatal("failed to set user defined type")
			}
		case "hello":
			if *v != "world" {
				t.Fatal("failed to set user defined type")
			}
		default:
			break
		}
	}
	svc.DeleteObject(TEST_BUCKET, TEST_KEY)
	svc.DeleteObject(TEST_BUCKET, TEST_COPY_KEY)
}

func Test_RenameObject(t *testing.T) {
	//non-cryption
	svc := NewS3()
	err := svc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}

	TEST_COPY_KEY := "COPY:" + TEST_KEY
	input1 := &s3.CopyObjectInput{
		Bucket:     aws.String(TEST_BUCKET),
		CopySource: aws.String(TEST_BUCKET + "/" + TEST_KEY),
		Key:        aws.String(TEST_COPY_KEY),
	}
	_, err = svc.Client.CopyObject(input1)
	if err != nil {
		t.Fatal("Copy Object err:", err)
	}

	TEST_RENAME_KEY := "RENAME:" + TEST_KEY
	input2 := &s3.RenameObjectInput{
		Bucket:     aws.String(TEST_BUCKET),
		CopySource: aws.String(TEST_BUCKET + "/" + TEST_KEY),
		Key:        aws.String(TEST_RENAME_KEY),
	}
	_, err = svc.Client.RenameObject(input2)
	if err != nil {
		t.Fatal("Rename Object err:", err)
	}

	//verify them
	v1, err := svc.GetObject(TEST_BUCKET, TEST_COPY_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	v2, err := svc.GetObject(TEST_BUCKET, TEST_RENAME_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	if v1 != v2 {
		t.Fatal("Rename result is not the same.")
	}

	//clean up
	svc.DeleteObject(TEST_BUCKET, TEST_COPY_KEY)
	svc.DeleteObject(TEST_BUCKET, TEST_RENAME_KEY)
}

func Test_RenameObjectErrFloder(t *testing.T) {
	//non-cryption
	svc := NewS3()
	err := svc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}

	TEST_RENAME_KEY := "RENAME:" + TEST_KEY +"/"
	input := &s3.RenameObjectInput{
		Bucket:     aws.String(TEST_BUCKET),
		CopySource: aws.String(TEST_BUCKET + "/" + TEST_KEY),
		Key:        aws.String(TEST_RENAME_KEY),
	}
	_, err = svc.Client.RenameObject(input)
	if err == nil {
		t.Fatal("Rename Object with floder:", err)
	}
	
	//clean up
	svc.DeleteObject(TEST_BUCKET, TEST_RENAME_KEY)
}

func Test_Object_Append(t *testing.T) {
	sc := NewS3()
	sc.DeleteObject(TEST_BUCKET, TEST_KEY)
	sc.DeleteBucket(TEST_BUCKET)
	sc.MakeBucket(TEST_BUCKET)
	var nextPos int64
	var err error
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

	nextPos, err = sc.AppendObject(TEST_BUCKET, TEST_KEY, TEST_VALUE+"APPEND", nextPos)
	if err != nil {
		t.Fatal("AppendObject err:", err)
	}
	v, err = sc.GetObject(TEST_BUCKET, TEST_KEY)
	if v != TEST_VALUE+TEST_VALUE+"APPEND" {
		t.Fatal("GetObject err: value is:", v, ", but should be:", TEST_VALUE+TEST_VALUE+"APPEND")
	}
	t.Log("AppendObject success!")
	err = sc.DeleteObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Log("DeleteObject err:", err)
	}
}

func Test_Object_End(t *testing.T) {
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

var (
	GetObjectAllowPolicy = `{
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

	GetObjectDenyPolicy = `{
			"Version": "2012-10-17",
			"Statement": [{
			"Effect": "Deny",
			"Principal": {"AWS":["*"]},
			"Action": ["s3:GetObject"],
			"Resource": [
				"arn:aws:s3:::` + TEST_BUCKET + `/*"
			]
			}]
		}`

	EmptyPolicy = `{"Version": "2012-10-17"}`
)

// Test different situation with access policy when anonymous access;
// 1. BucketPolicy Allow;	BucketACL PublicRead;	ObjectACL PublicRead;	GetObject should be OK;
// 2. BucketPolicy Allow;	BucketACL PublicRead;	ObjectACL Private;		GetObject should be OK;
// 3. BucketPolicy Allow;	BucketACL Private;	ObjectACL Private;	GetObject should be OK;
// 4. BucketPolicy Allow;	BucketACL Private;	ObjectACL PublicRead; 	GetObject should be OK;
// 5. BucketPolicy Deny;	BucketACL PublicRead;	ObjectACL PublicRead; 	GetObject should be Failed;
// 6. BucketPolicy Deny;	BucketACL PublicRead;	ObjectACL Private; 		GetObject should be Failed;
// 7. BucketPolicy Deny;	BucketACL Private;	ObjectACL Private;	GetObject should be Failed;
// 8. BucketPolicy Deny;	BucketACL Private;	ObjectACL PublicRead; 	GetObject should be Failed;
// 9. BucketPolicy Pass;	BucketACL PublicRead;	ObjectACL PublicRead; 	GetObject should be OK;
// 10.BucketPolicy Pass;	BucketACL PublicRead;	ObjectACL Private; 		GetObject should be Failed;
// 11.BucketPolicy Pass;	BucketACL Private;	ObjectACL Private;	GetObject should be Failed;
// 12.BucketPolicy Pass;	BucketACL Private;	ObjectACL PublicRead;	GetObject should be OK;
func Test_GetObjectByAnonymous(t *testing.T) {
	sc := NewS3()
	sc.CleanEnv()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	err = sc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
		panic(err)
	}

	// Situation 1:BucketPolicy Allow;	BucketACL PublicRead;	ObjectACL PublicRead;	GetObject should be OK;
	accessPolicyGroup1 := AccessPolicyGroup{GetObjectAllowPolicy, BucketCannedACLPublicRead, ObjectCannedACLPublicRead}
	err = sc.TestAnonymousAccessResult(accessPolicyGroup1, http.StatusOK)
	if err != nil {
		t.Log("Anonymous access situation 1: GetObjectAllowPolicy, BucketCannedACLPublicRead, ObjectCannedACLPublicRead Failed.")
		t.Fatal(err)
	}

	// Situation 2. BucketPolicy Allow;	BucketACL PublicRead;	ObjectACL Private;		GetObject should be OK;
	accessPolicyGroup2 := AccessPolicyGroup{GetObjectAllowPolicy, BucketCannedACLPublicRead, ObjectCannedACLPrivate}
	err = sc.TestAnonymousAccessResult(accessPolicyGroup2, http.StatusOK)
	if err != nil {
		t.Log("Anonymous access situation 2: GetObjectAllowPolicy, BucketCannedACLPublicRead, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}

	// Situation 3. BucketPolicy Allow;	BucketACL Private;		ObjectACL Private; 		GetObject should be OK;
	accessPolicyGroup3 := AccessPolicyGroup{GetObjectAllowPolicy, BucketCannedACLPrivate, ObjectCannedACLPrivate}
	err = sc.TestAnonymousAccessResult(accessPolicyGroup3, http.StatusOK)
	if err != nil {
		t.Log("Anonymous access situation 3: GetObjectAllowPolicy, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}

	// Situation 4. BucketPolicy Allow;	BucketACL Private;		ObjectACL PublicRead; 	GetObject should be OK;
	accessPolicyGroup4 := AccessPolicyGroup{GetObjectAllowPolicy, BucketCannedACLPrivate, ObjectCannedACLPublicRead}
	err = sc.TestAnonymousAccessResult(accessPolicyGroup4, http.StatusOK)
	if err != nil {
		t.Log("Anonymous access situation 4: GetObjectAllowPolicy, BucketCannedACLPrivate, ObjectCannedACLPublicRead Failed.")
		t.Fatal(err)
	}

	// Situation 5. BucketPolicy Deny;	BucketACL PublicRead;	ObjectACL PublicRead; 	GetObject should be Failed;
	accessPolicyGroup5 := AccessPolicyGroup{GetObjectDenyPolicy, BucketCannedACLPublicRead, ObjectCannedACLPrivate}
	err = sc.TestAnonymousAccessResult(accessPolicyGroup5, http.StatusForbidden)
	if err != nil {
		t.Log("Anonymous access situation 5: GetObjectDenyPolicy, BucketCannedACLPublicRead, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}

	// 6. BucketPolicy Deny;	BucketACL PublicRead;	ObjectACL Private; 		GetObject should be Failed;
	accessPolicyGroup6 := AccessPolicyGroup{GetObjectDenyPolicy, BucketCannedACLPublicRead, ObjectCannedACLPrivate}
	err = sc.TestAnonymousAccessResult(accessPolicyGroup6, http.StatusForbidden)
	if err != nil {
		t.Log("Anonymous access situation 6: GetObjectDenyPolicy, BucketCannedACLPublicRead, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}

	// 7. BucketPolicy Deny;	BucketACL Private;		ObjectACL Private; 		GetObject should be Failed;
	accessPolicyGroup7 := AccessPolicyGroup{GetObjectDenyPolicy, BucketCannedACLPrivate, ObjectCannedACLPrivate}
	err = sc.TestAnonymousAccessResult(accessPolicyGroup7, http.StatusForbidden)
	if err != nil {
		t.Log("Anonymous access situation 7: GetObjectDenyPolicy, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}

	// 8. BucketPolicy Deny;	BucketACL Private;		ObjectACL PublicRead; 	GetObject should be Failed;
	accessPolicyGroup8 := AccessPolicyGroup{GetObjectDenyPolicy, BucketCannedACLPrivate, ObjectCannedACLPublicRead}
	err = sc.TestAnonymousAccessResult(accessPolicyGroup8, http.StatusForbidden)
	if err != nil {
		t.Log("Anonymous access situation 8: GetObjectDenyPolicy, BucketCannedACLPrivate, ObjectCannedACLPublicRead Failed.")
		t.Fatal(err)
	}

	// 9. BucketPolicy Pass;	BucketACL PublicRead;	ObjectACL PublicRead; 	GetObject should be OK;
	accessPolicyGroup9 := AccessPolicyGroup{EmptyPolicy, BucketCannedACLPublicRead, ObjectCannedACLPublicRead}
	err = sc.TestAnonymousAccessResult(accessPolicyGroup9, http.StatusOK)
	if err != nil {
		t.Log("Anonymous access situation 9: mptyPolicy, BucketCannedACLPublicRead, ObjectCannedACLPublicRead Failed.")
		t.Fatal(err)
	}

	// 10.BucketPolicy Pass;	BucketACL PublicRead;	ObjectACL Private; 		GetObject should be Failed;
	accessPolicyGroup10 := AccessPolicyGroup{EmptyPolicy, BucketCannedACLPublicRead, ObjectCannedACLPrivate}
	err = sc.TestAnonymousAccessResult(accessPolicyGroup10, http.StatusForbidden)
	if err != nil {
		t.Log("Anonymous access situation 10: EmptyPolicy, BucketCannedACLPublicRead, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}

	// 11.BucketPolicy Pass;	BucketACL Private;		ObjectACL Private; 		GetObject should be Failed;
	accessPolicyGroup11 := AccessPolicyGroup{EmptyPolicy, BucketCannedACLPrivate, ObjectCannedACLPrivate}
	err = sc.TestAnonymousAccessResult(accessPolicyGroup11, http.StatusForbidden)
	if err != nil {
		t.Log("Anonymous access situation 11: EmptyPolicy, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}

	// 12.BucketPolicy Pass;	BucketACL Private;		ObjectACL PublicRead; 	GetObject should be OK;
	accessPolicyGroup12 := AccessPolicyGroup{EmptyPolicy, BucketCannedACLPrivate, ObjectCannedACLPublicRead}
	err = sc.TestAnonymousAccessResult(accessPolicyGroup12, http.StatusOK)
	if err != nil {
		t.Log("Anonymous access situation 12: EmptyPolicy, BucketCannedACLPrivate, ObjectCannedACLPublicRead  Failed.")
		t.Fatal(err)
	}

	sc.CleanEnv()

}

