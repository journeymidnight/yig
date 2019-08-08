package _go

import (
	"testing"

	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	. "github.com/journeymidnight/yig/test/go/lib"
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

	partCount := 10
	completedUpload := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, partCount),
	}

	for i := 0; i < partCount; i++ {
		partNumber := int64(i + 1)
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

func Test_MultipartUploadWithoutMD5(t *testing.T) {
	sc := NewS3WithoutMD5()
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

	partCount := 10
	completedUpload := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, partCount),
	}

	for i := 0; i < partCount; i++ {
		partNumber := int64(i + 1)
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

func Test_CopyObjectPart(t *testing.T) {
	svc := NewS3()
	defer svc.CleanEnv()
	svc.CleanEnv()
	err := svc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	uploadId, err := svc.CreateMultiPartUpload(TEST_BUCKET, TEST_KEY, s3.ObjectStorageClassStandard)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	partCount := 10
	completedUpload := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, partCount),
	}
	for i := 0; i < partCount; i++ {
		partNumber := int64(i + 1)
		etag, err := svc.UploadPart(TEST_BUCKET, TEST_KEY, GenMinimalPart(), uploadId, partNumber)
		if err != nil {
			t.Fatal("UploadPart err:", err)
			panic(err)
		}
		completedUpload.Parts[i] = &s3.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int64(partNumber),
		}
	}
	err = svc.CompleteMultiPartUpload(TEST_BUCKET, TEST_KEY, uploadId, completedUpload)
	if err != nil {
		t.Fatal("CompleteMultiPartUpload err:", err)
		err = svc.AbortMultiPartUpload(TEST_BUCKET, TEST_KEY, uploadId)
		if err != nil {
			t.Fatal("AbortMultiPartUpload err:", err)
		}
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

func Test_CopyObjectPartWithoutMD5(t *testing.T) {
	svc := NewS3WithoutMD5()
	defer svc.CleanEnv()
	svc.CleanEnv()
	err := svc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	uploadId, err := svc.CreateMultiPartUpload(TEST_BUCKET, TEST_KEY, s3.ObjectStorageClassStandard)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	partCount := 10
	completedUpload := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, partCount),
	}

	for i := 0; i < partCount; i++ {
		partNumber := int64(i + 1)
		etag, err := svc.UploadPart(TEST_BUCKET, TEST_KEY, GenMinimalPart(), uploadId, partNumber)
		if err != nil {
			t.Fatal("UploadPart err:", err)
			panic(err)
		}
		completedUpload.Parts[i] = &s3.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int64(partNumber),
		}
	}
	err = svc.CompleteMultiPartUpload(TEST_BUCKET, TEST_KEY, uploadId, completedUpload)
	if err != nil {
		t.Fatal("CompleteMultiPartUpload err:", err)
		err = svc.AbortMultiPartUpload(TEST_BUCKET, TEST_KEY, uploadId)
		if err != nil {
			t.Fatal("AbortMultiPartUpload err:", err)
		}
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