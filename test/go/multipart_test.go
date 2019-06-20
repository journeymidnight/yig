package _go

import (
	"testing"
	. "github.com/journeymidnight/yig/test/go/lib"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/aws-sdk-go/aws"
)

func Test_MultipartUpload(t *testing.T) {
	sc := NewS3()
	defer sc.CleanEnv()
	sc.CleanEnv()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	uploadId, err := sc.CreateMultiPartUpload(TEST_BUCKET, TEST_KEY, s3.ObjectStorageClassStandard)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	partCount := 3
	completedUpload := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, partCount),
	}

	for i := 0; i < partCount; i++ {
		partNumber := int64(i+1);
		etag, err := sc.UploadPart(TEST_BUCKET, TEST_KEY, GenMinimalPart(), uploadId, partNumber)
		if err != nil {
			t.Fatal("UploadPart err:", err)
			panic(err)
		}
		completedUpload.Parts[i] = &s3.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int64(partNumber),
		}
	}

	err = sc.CompleteMultiPartUpload(TEST_BUCKET, TEST_KEY, uploadId, completedUpload)
	if err != nil {
		t.Fatal("CompleteMultiPartUpload err:", err)
		err = sc.AbortMultiPartUpload(TEST_BUCKET, TEST_KEY, uploadId)
		if err != nil {
			t.Fatal("AbortMultiPartUpload err:", err)
		}
	}

}
