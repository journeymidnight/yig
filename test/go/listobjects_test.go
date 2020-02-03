package _go

import (
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	. "github.com/journeymidnight/yig/test/go/lib"
	"testing"
)

type TestListObjectsCase struct {
	BucketName   string
	Key          string
	Value        string
	StorageClass string
	Expected     string
}

func Test_ListObjects_With_StorageClass(t *testing.T)  {
	testCases := []TestListObjectsCase{
		{TEST_BUCKET, TEST_KEY, TEST_VALUE, s3.ObjectStorageClassStandard, s3.ObjectStorageClassStandard},
		{TEST_BUCKET, TEST_KEY, TEST_VALUE, s3.ObjectStorageClassStandardIa, s3.ObjectStorageClassStandardIa},
		{TEST_BUCKET, TEST_KEY, TEST_VALUE, s3.ObjectStorageClassGlacier, s3.ObjectStorageClassGlacier},
	}
	sc := NewS3()
	defer sc.CleanEnv()
	for _, c := range testCases {
		sc.CleanEnv()
		err := sc.MakeBucket(c.BucketName)
		if err != nil {
			t.Fatal("MakeBucket err:", err)
		}
		err = sc.PutObjectWithStorageClass(c.BucketName, c.Key, c.Value, c.StorageClass)
		if err != nil {
			t.Fatal("PutObjectWithStorageClass err:", err)
		}
		objects, err := sc.ListObjects(c.BucketName)
		for _, object := range objects {
			if *object.Key == c.Key {
				if *object.StorageClass != c.StorageClass {
					t.Fatal("StorageClass is not correct. out:", *object.StorageClass, "expected:", c.Expected)
				}
			}
		}
	}
}
