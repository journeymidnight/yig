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

func Test_Object(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	defer sc.CleanEnv()
	err = sc.PutObject(TestBucket, TestKey, TestValue)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	t.Log("PutObject Success!")

	sc2 := NewS3WithoutMD5()
	err = sc2.PutObject(TestBucket, TestKey, TestValue)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	t.Log("PutObject Success!")

	err = sc.HeadObject(TestBucket, TestKey)
	if err != nil {
		t.Fatal("HeadBucket err:", err)
	}
	t.Log("HeadObject Success!")

	v, err := sc.GetObject(TestBucket, TestKey)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	if v != TestValue {
		t.Fatal("GetObject err: value is:", v, ", but should be:", TestValue)
	}
	t.Log("GetObject Success value:", v)

	err = sc.DeleteObject(TestBucket, TestKey)
	if err != nil {
		t.Fatal("DeleteObject err:", err)
	}
	err = sc.HeadObject(TestBucket, TestKey)
	if err == nil {
		t.Fatal("HeadObject err:", err)
	}
	t.Log("DeleteObject Success!")

	pbi := &PostObjectInput{
		Url:        fmt.Sprintf("http://s3.test.com:8080/%s", TestBucket),
		Bucket:     TestBucket,
		ObjName:    TestKey,
		Expiration: time.Now().UTC().Add(time.Duration(1 * time.Hour)),
		Date:       time.Now().UTC(),
		Region:     "r",
		AK:         "hehehehe",
		SK:         "hehehehe",
		FileSize:   1024,
	}

	err = sc.PostObject(pbi)
	if err != nil {
		t.Fatal("PostObject err:", err)
	}
	t.Log("PostObject Success!")

	url, err := sc.GetObjectPreSigned(TestBucket, TestKey, 5*time.Second)
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

func Test_CopyObjectWithoutMD5(t *testing.T) {
	TEST_COPY_KEY := "COPYED:" + TestKey
	svc := NewS3WithoutMD5()
	err := svc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	defer func() {
		//clean up
		svc.DeleteObject(TestBucket, TestKey)
		svc.DeleteObject(TestBucket, TEST_COPY_KEY)
		svc.DeleteBucket(TestBucket)
	}()
	err = svc.PutObject(TestBucket, TestKey, TestValue)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}

	input := &s3.CopyObjectInput{
		Bucket:     aws.String(TestBucket),
		CopySource: aws.String(TestBucket + "/" + TestKey),
		Key:        aws.String(TEST_COPY_KEY),
	}
	_, err = svc.Client.CopyObject(input)
	if err != nil {
		t.Fatal("Copy Object err:", err)
	}

	//verify them
	v1, err := svc.GetObject(TestBucket, TestKey)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	v2, err := svc.GetObject(TestBucket, TEST_COPY_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	if v1 != v2 {
		t.Fatal("Copyed result is not the same.")
	}
}

func Test_CopyObject(t *testing.T) {
	TEST_COPY_KEY := "COPYED:" + TestKey
	//non-cryption
	svc := NewS3()
	err := svc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	defer func() {
		//clean up
		svc.DeleteObject(TestBucket, TestKey)
		svc.DeleteObject(TestBucket, TEST_COPY_KEY)
		svc.DeleteBucket(TestBucket)
	}()
	err = svc.PutObject(TestBucket, TestKey, TestValue)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}

	input := &s3.CopyObjectInput{
		Bucket:     aws.String(TestBucket),
		CopySource: aws.String(TestBucket + "/" + TestKey),
		Key:        aws.String(TEST_COPY_KEY),
	}
	_, err = svc.Client.CopyObject(input)
	if err != nil {
		t.Fatal("Copy Object err:", err)
	}

	//verify them
	v1, err := svc.GetObject(TestBucket, TestKey)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	v2, err := svc.GetObject(TestBucket, TEST_COPY_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	if v1 != v2 {
		t.Fatal("Copyed result is not the same.")
	}
}

func Test_CopyObjectWithReplace(t *testing.T) {
	TEST_COPY_KEY := "COPYED:" + TestKey
	svc := NewS3()
	err := svc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	defer func() {
		//clean up
		svc.DeleteObject(TestBucket, TestKey)
		svc.DeleteObject(TestBucket, TEST_COPY_KEY)
		svc.DeleteBucket(TestBucket)
	}()

	err = svc.PutObject(TestBucket, TestKey, TestValue)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}

	input := &s3.CopyObjectInput{
		Bucket:            aws.String(TestBucket),
		CopySource:        aws.String(TestBucket + "/" + TestKey),
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
		Bucket: aws.String(TestBucket),
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
}

func Test_RenameObject(t *testing.T) {
	TEST_COPY_KEY := "COPY:" + TestKey
	TEST_RENAME_KEY := "RENAME:" + TestKey

	//non-cryption
	svc := NewS3()
	err := svc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	defer func() {
		//clean up
		svc.DeleteObject(TestBucket, TestKey)
		svc.DeleteObject(TestBucket, TEST_COPY_KEY)
		svc.DeleteObject(TestBucket, TEST_RENAME_KEY)
		svc.DeleteBucket(TestBucket)
	}()

	err = svc.PutObject(TestBucket, TestKey, TestValue)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}

	input1 := &s3.CopyObjectInput{
		Bucket:     aws.String(TestBucket),
		CopySource: aws.String(TestBucket + "/" + TestKey),
		Key:        aws.String(TEST_COPY_KEY),
	}
	_, err = svc.Client.CopyObject(input1)
	if err != nil {
		t.Fatal("Copy Object err:", err)
	}

	input2 := &s3.RenameObjectInput{
		Bucket:          aws.String(TestBucket),
		RenameSourceKey: aws.String(TestKey),
		Key:             aws.String(TEST_RENAME_KEY),
	}
	_, err = svc.Client.RenameObject(input2)
	if err != nil {
		t.Fatal("Rename Object err:", err)
	}

	//verify them
	v1, err := svc.GetObject(TestBucket, TEST_COPY_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	v2, err := svc.GetObject(TestBucket, TEST_RENAME_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	if v1 != v2 {
		t.Fatal("Rename result is not the same.")
	}
}

func Test_RenameObjectWithSameName(t *testing.T) {
	TEST_SAME_KEY := "SAME:" + TestKey

	//non-cryption
	svc := NewS3()
	err := svc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	delFn := func(sc *S3Client) {
		//clean up
		svc.DeleteObject(TestBucket, TEST_SAME_KEY)
		svc.DeleteObject(TestBucket, TestKey)
		svc.DeleteBucket(TestBucket)
	}
	defer delFn(svc)

	err = svc.PutObject(TestBucket, TestKey, TestValue)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}

	input1 := &s3.CopyObjectInput{
		Bucket:     aws.String(TestBucket),
		CopySource: aws.String(TestBucket + "/" + TestKey),
		Key:        aws.String(TEST_SAME_KEY),
	}
	_, err = svc.Client.CopyObject(input1)
	if err != nil {
		t.Fatal("Copy Object err:", err)
	}

	input2 := &s3.RenameObjectInput{
		Bucket:          aws.String(TestBucket),
		RenameSourceKey: aws.String(TestKey),
		Key:             aws.String(TEST_SAME_KEY),
	}
	_, err = svc.Client.RenameObject(input2)
	if err == nil {
		t.Fatal("Rename Object err:", err)
	}
}

func Test_RenameObjectErrFolder(t *testing.T) {
	TEST_RENAME_KEY := "RENAME:" + TestKey + "/"

	//non-cryption
	svc := NewS3()
	err := svc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	delFn := func(sc *S3Client) {
		//clean up
		svc.DeleteObject(TestBucket, TEST_RENAME_KEY)
		svc.DeleteObject(TestBucket, TestKey)
		svc.DeleteBucket(TestBucket)
	}
	defer delFn(svc)
	err = svc.PutObject(TestBucket, TestKey, TestValue)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}

	input := &s3.RenameObjectInput{
		Bucket:          aws.String(TestBucket),
		RenameSourceKey: aws.String(TestKey),
		Key:             aws.String(TEST_RENAME_KEY),
	}
	_, err = svc.Client.RenameObject(input)
	if err == nil {
		t.Fatal("Rename Object with floder:", err)
	}
}

func Test_Object_Append(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	delFn := func(sc *S3Client) {
		sc.DeleteObject(TestBucket, TestKey)
		sc.DeleteBucket(TestBucket)
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

var (
	GetObjectAllowPolicy = `{
			"Version": "2012-10-17",
			"Statement": [{
			"Effect": "Allow",
			"Principal": {"AWS":["*"]},
			"Action": ["s3:GetObject"],
			"Resource": [
				"arn:aws:s3:::` + TestBucket + `/*"
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
				"arn:aws:s3:::` + TestBucket + `/*"
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
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	err = sc.PutObject(TestBucket, TestKey, TestValue)
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

type MetaTestUnit struct {
	WebsiteConfiguration *s3.MetaConfiguration
	Buckets              []string
	Objects              []MetaObjectInput
	Cases                []MetaCase
}

type MetaObjectInput struct {
	Bucket string
	Key    string
	value  string
}

type MetaCase struct {
	ExpectedMeta map[string]string
}

var testMetaUnits = []MetaTestUnit{
	{
		WebsiteConfiguration: &s3.MetaConfiguration{
			Headers: []*s3.MetaData{
				{
					Key:   aws.String("Content-Type"),
					Value: aws.String("image/jpeg"),
				},
				{
					Key:   aws.String("Cache-Control"),
					Value: aws.String("noCache"),
				},
				{
					Key:   aws.String("Content-Disposition"),
					Value: aws.String("TestContentDisposition"),
				},
				{
					Key:   aws.String("Content-Encoding"),
					Value: aws.String("utf-8"),
				},
				{
					Key:   aws.String("Content-Language"),
					Value: aws.String("golang"),
				},
				{
					Key:   aws.String("Expires"),
					Value: aws.String("800"),
				},
				{
					Key:   aws.String("X-Amz-Meta-Hehehehe"),
					Value: aws.String("hehehehe"),
				},
				{
					Key:   aws.String("X-Amz-Meta-Hello"),
					Value: aws.String("world"),
				},
			},
			VersionID: aws.String("2019/10/22"),
		},
		Buckets: []string{TestBucket},
		Objects: []MetaObjectInput{
			{TestBucket, TestKey, TestValue},
		},
		Cases: []MetaCase{
			{
				ExpectedMeta: map[string]string{
					"Content-Type":        "image/jpeg",
					"Cache-Control":       "noCache",
					"Content-Disposition": "TestContentDisposition",
					"Content-Encoding":    "utf-8",
					"Content-Language":    "golang",
					"Expires":             "800",
					// The SDK will automatically erase the previous Amazon standard headers.
					"Hehehehe": "hehehehe",
					"Hello":    "world",
				},
			},
		},
	},
}

func CleanMetaUnits(sc *S3Client) {
	for _, unit := range testMetaUnits {
		cleanMeta(sc, unit)
	}
}

func cleanMeta(sc *S3Client, unit MetaTestUnit) {
	for _, o := range unit.Objects {
		sc.DeleteObject(o.Bucket, o.Key)
	}
	for _, b := range unit.Buckets {
		sc.DeleteBucket(b)
	}
}

func Test_PutObjectMeta(t *testing.T) {
	sc := NewS3()
	CleanMetaUnits(sc)
	defer CleanMetaUnits(sc)
	for _, unit := range testMetaUnits {
		for _, b := range unit.Buckets {
			err := sc.MakeBucket(b)
			if err != nil {
				t.Fatal("MakeBucket err:", err)
			}
			for _, o := range unit.Objects {
				err := sc.PutObject(o.Bucket, o.Key, o.value)
				if err != nil {
					t.Fatal("PutObject err:", err)
				}
				input := &s3.PutObjectMetaInput{
					Bucket:            aws.String(o.Bucket),
					Key:               aws.String(o.Key),
					MetaConfiguration: unit.WebsiteConfiguration,
				}
				_, err = sc.Client.PutObjectMeta(input)
				if err != nil {
					t.Fatal("Put Object MetaData with :", err)
				}
				params := &s3.HeadObjectInput{
					Bucket: aws.String(b),
					Key:    aws.String(o.Key),
				}
				headResult, err := sc.Client.HeadObject(params)
				if err != nil {
					t.Fatal("Head object failed")
				}

				t.Log("ResultMetadata:", headResult.Metadata)
				for _, c := range unit.Cases {
					for k, v := range headResult.Metadata {
						if *v != c.ExpectedMeta[k] {
							t.Fatal("failed to set", k)
						}
					}
				}
			}
		}
	}
}

var ObjectKeys = []string{
	TestKey,
	TestKeySpecial,
	TestKey + "/" + TestKey,
	TestKey + "/" + TestValue,
	TestValue + "/" + TestKey,
	TestValue + "/" + TestValue,
}

func Test_ListObjects(t *testing.T) {
	sc := NewS3()
	delFn := func(sc *S3Client) {
		for _, k := range ObjectKeys {
			sc.DeleteObject(TestBucket, k)
		}
		sc.DeleteBucket(TestBucket)
	}
	delFn(sc)
	defer delFn(sc)
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
	}
	for _, k := range ObjectKeys {
		err = sc.PutObject(TestBucket, k, TestValue)
		if err != nil {
			t.Fatal("PutObject err:", err)
		}
	}
	out, err := sc.ListObjects(TestBucket, "", "", 1000)
	if err != nil {
		t.Fatal("ListObjects err:", err)
	}
	PrintListResult(t, out)
	if len(out.Contents) != 2 {
		t.Fatal("ListObjects err: result Content length should be 2 but not", len(out.Contents))
	}
	if len(out.CommonPrefixes) != 2 {
		t.Fatal("ListObjects err: result CommonPrefixes length should be 2, but not", len(out.CommonPrefixes))
	}

	// TODO: Add more validation
	out, err = sc.ListObjects(TestBucket, "", "", 1)
	if err != nil {
		t.Fatal("ListObjects err:", err)
	}
	PrintListResult(t, out)

	out, err = sc.ListObjects(TestBucket, *out.NextMarker, "", 1)
	if err != nil {
		t.Fatal("ListObjects err:", err)
	}
	PrintListResult(t, out)
}

func PrintListResult(t *testing.T, out *s3.ListObjectsOutput) {
	for i, o := range out.Contents {
		if o.Key != nil {
			t.Log("Object", i, ":", *o.Key)
		}
	}
	for i, o := range out.CommonPrefixes {
		if o.Prefix != nil {
			t.Log("CommonPrefix", i, ":", *o.Prefix)
		}
	}

	if out.IsTruncated == nil {
		t.Log("IsTruncated:", nil)
	} else {
		t.Log("IsTruncated:", *out.IsTruncated)
	}

	if out.NextMarker == nil {
		t.Log("NextMarker:", nil)
	} else {
		t.Log("NextMarker:", *out.NextMarker)
	}

}
