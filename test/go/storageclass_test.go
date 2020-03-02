package _go

import (
	"testing"

	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	. "github.com/journeymidnight/yig/test/go/lib"
)

type TestStorageClassCase struct {
	BucketName   string
	Key          string
	Value        []byte
	StorageClass string
	Expected     string
}

func Test_PutObject_With_StorageClass(t *testing.T) {
	testCases := []TestStorageClassCase{
		{TEST_BUCKET, TEST_KEY, []byte(TEST_VALUE), s3.ObjectStorageClassStandard, s3.ObjectStorageClassStandard},
		{TEST_BUCKET, TEST_KEY, []byte(TEST_VALUE), s3.ObjectStorageClassStandardIa, s3.ObjectStorageClassStandardIa},
		{TEST_BUCKET, TEST_KEY, []byte(TEST_VALUE), s3.ObjectStorageClassGlacier, s3.ObjectStorageClassGlacier},
	}
	sc := NewS3()
	defer sc.CleanEnv()
	for _, c := range testCases {
		sc.CleanEnv()
		err := sc.MakeBucket(c.BucketName)
		if err != nil {
			t.Fatal("MakeBucket err:", err)
			panic(err)
		}
		err = sc.PutObjectWithStorageClass(c.BucketName, c.Key, string(c.Value), c.StorageClass)
		if err != nil {
			t.Fatal("PutObjectWithStorageClass err:", err)
			panic(err)
		}
		if c.StorageClass != s3.ObjectStorageClassGlacier {
			out, err := sc.GetObjectOutPut(c.BucketName, c.Key)
			if err != nil {
				t.Fatal("GetObjectOutPut err:", err)
				panic(err)
			}
			if *out.StorageClass != c.Expected {
				t.Fatal("StorageClass is not correct. out:", *out.StorageClass, "expected:", c.Expected)
			}
		}
	}
}

func Test_MultipartUpload_With_StorageClass(t *testing.T) {
	testCases := []TestStorageClassCase{
		{TEST_BUCKET, TEST_KEY, GenMinimalPart(), s3.ObjectStorageClassStandard, s3.ObjectStorageClassStandard},
		{TEST_BUCKET, TEST_KEY, GenMinimalPart(), s3.ObjectStorageClassStandardIa, s3.ObjectStorageClassStandardIa},
		{TEST_BUCKET, TEST_KEY, GenMinimalPart(), s3.ObjectStorageClassGlacier, s3.ObjectStorageClassGlacier},
	}
	sc := NewS3()
	defer sc.CleanEnv()
	for _, c := range testCases {
		sc.CleanEnv()
		err := sc.MakeBucket(c.BucketName)
		if err != nil {
			t.Fatal("MakeBucket err:", err)
			panic(err)
		}

		uploadId, err := sc.CreateMultiPartUpload(c.BucketName, c.Key, c.Expected)
		if err != nil {
			t.Fatal("MakeBucket err:", err)
			panic(err)
		}

		partCount := 2
		completedUpload := &s3.CompletedMultipartUpload{
			Parts: make([]*s3.CompletedPart, partCount),
		}

		for i := 0; i < partCount; i++ {
			partNumber := int64(i + 1)
			etag, err := sc.UploadPart(c.BucketName, c.Key, c.Value, uploadId, partNumber)
			if err != nil {
				t.Fatal("UploadPart err:", err)
				panic(err)
			}
			completedUpload.Parts[i] = &s3.CompletedPart{
				ETag:       aws.String(etag),
				PartNumber: aws.Int64(partNumber),
			}
		}

		err = sc.CompleteMultiPartUpload(c.BucketName, c.Key, uploadId, completedUpload)
		if err != nil {
			t.Fatal("CompleteMultiPartUpload err:", err)
			err = sc.AbortMultiPartUpload(c.BucketName, c.Key, uploadId)
			if err != nil {
				t.Fatal("AbortMultiPartUpload err:", err)
			}
		}

		if c.StorageClass != s3.ObjectStorageClassGlacier {
			out, err := sc.GetObjectOutPut(c.BucketName, c.Key)
			if err != nil {
				t.Fatal("GetObjectOutPut err:", err)
				panic(err)
			}
			if *out.StorageClass != c.Expected {
				t.Fatal("StorageClass is not correct. out:", *out.StorageClass, "expected:", c.Expected)
			}
		}
	}
}

func Test_Change_StorageClass(t *testing.T) {
	testCases := []TestStorageClassCase{
		{TEST_BUCKET, TEST_KEY, []byte(TEST_VALUE), s3.ObjectStorageClassStandardIa, s3.ObjectStorageClassStandardIa},
	}
	sc := NewS3()
	defer sc.CleanEnv()
	for _, c := range testCases {
		sc.CleanEnv()
		err := sc.MakeBucket(c.BucketName)
		if err != nil {
			t.Fatal("MakeBucket err:", err)
			panic(err)
		}
		err = sc.PutObjectWithStorageClass(c.BucketName, c.Key, string(c.Value), s3.ObjectStorageClassStandard)
		if err != nil {
			t.Fatal("PutObjectWithStorageClass err:", err)
			panic(err)
		}

		err = sc.ChangeObjectStorageClass(c.BucketName, c.Key, c.StorageClass)
		if err != nil {
			t.Fatal("ChangeObjectStorageClass err:", err)
			panic(err)
		}

		out, err := sc.GetObjectOutPut(c.BucketName, c.Key)
		if err != nil {
			t.Fatal("GetObjectOutPut err:", err)
			panic(err)
		}
		if *out.StorageClass != c.Expected {
			t.Fatal("StorageClass is not correct. out:", *out.StorageClass, "expected:", c.Expected)
		}
	}
}
