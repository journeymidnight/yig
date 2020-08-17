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
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	uploadId, err := sc.CreateMultiPartUpload(TestBucket, TestKey, s3.ObjectStorageClassStandard)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	partCount := 3
	completedUpload := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, partCount),
	}

	for i := 0; i < partCount; i++ {
		partNumber := int64(i + 1)
		etag, err := sc.UploadPart(TestBucket, TestKey, GenMinimalPart(), uploadId, partNumber)
		if err != nil {
			t.Fatal("UploadPart err:", err)
			panic(err)
		}
		completedUpload.Parts[i] = &s3.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int64(partNumber),
		}
	}

	err = sc.CompleteMultiPartUpload(TestBucket, TestKey, uploadId, completedUpload)
	if err != nil {
		t.Fatal("CompleteMultiPartUpload err:", err)
		err = sc.AbortMultiPartUpload(TestBucket, TestKey, uploadId)
		if err != nil {
			t.Fatal("AbortMultiPartUpload err:", err)
		}
	}

}

func Test_MultipartUploadWithoutMD5(t *testing.T) {
	sc := NewS3WithoutMD5()
	defer sc.CleanEnv()
	sc.CleanEnv()
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	uploadId, err := sc.CreateMultiPartUpload(TestBucket, TestKey, s3.ObjectStorageClassStandard)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	partCount := 3
	completedUpload := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, partCount),
	}

	for i := 0; i < partCount; i++ {
		partNumber := int64(i + 1)
		etag, err := sc.UploadPart(TestBucket, TestKey, GenMinimalPart(), uploadId, partNumber)
		if err != nil {
			t.Fatal("UploadPart err:", err)
			panic(err)
		}
		completedUpload.Parts[i] = &s3.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int64(partNumber),
		}
	}

	err = sc.CompleteMultiPartUpload(TestBucket, TestKey, uploadId, completedUpload)
	if err != nil {
		t.Fatal("CompleteMultiPartUpload err:", err)
		err = sc.AbortMultiPartUpload(TestBucket, TestKey, uploadId)
		if err != nil {
			t.Fatal("AbortMultiPartUpload err:", err)
		}
	}
}

func Test_MultipartRename(t *testing.T) {
	sc := NewS3()
	TEST_COPY_KEY := "COPY:" + TestKey
	TEST_RENAME_KEY := "RENAME:" + TestKey
	delFn := func(sc *S3Client) {
		sc.DeleteObject(TestBucket, TestKey)
		sc.DeleteObject(TestBucket, TEST_COPY_KEY)
		sc.DeleteObject(TestBucket, TEST_RENAME_KEY)
		sc.DeleteBucket(TestBucket)
	}
	defer delFn(sc)
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	uploadId, err := sc.CreateMultiPartUpload(TestBucket, TestKey, s3.ObjectStorageClassStandard)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	partCount := 3
	completedUpload := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, partCount),
	}

	for i := 0; i < partCount; i++ {
		partNumber := int64(i + 1)
		etag, err := sc.UploadPart(TestBucket, TestKey, GenMinimalPart(), uploadId, partNumber)
		if err != nil {
			t.Fatal("UploadPart err:", err)
			panic(err)
		}
		completedUpload.Parts[i] = &s3.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int64(partNumber),
		}
	}

	err = sc.CompleteMultiPartUpload(TestBucket, TestKey, uploadId, completedUpload)
	if err != nil {
		t.Fatal("CompleteMultiPartUpload err:", err)
		err = sc.AbortMultiPartUpload(TestBucket, TestKey, uploadId)
		if err != nil {
			t.Fatal("AbortMultiPartUpload err:", err)
		}
	}

	input1 := &s3.CopyObjectInput{
		Bucket:     aws.String(TestBucket),
		CopySource: aws.String(TestBucket + "/" + TestKey),
		Key:        aws.String(TEST_COPY_KEY),
	}
	_, err = sc.Client.CopyObject(input1)
	if err != nil {
		t.Fatal("Copy Object err:", err)
	}

	input2 := &s3.RenameObjectInput{
		Bucket:          aws.String(TestBucket),
		RenameSourceKey: aws.String(TestKey),
		Key:             aws.String(TEST_RENAME_KEY),
	}
	_, err = sc.Client.RenameObject(input2)
	if err != nil {
		t.Fatal("Rename Object err:", err)
	}

	//verify them
	v1, err := sc.GetObject(TestBucket, TEST_COPY_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	v2, err := sc.GetObject(TestBucket, TEST_RENAME_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	if v1 != v2 {
		t.Fatal("Rename result is not the same.")
	}

}

func Test_CopyObjectPart(t *testing.T) {
	svc := NewS3()
	TEST_COPY_KEY := "COPYED:" + TestKey
	delFn := func(sc *S3Client) {
		sc.DeleteObject(TestBucket, TestKey)
		sc.DeleteObject(TestBucket, TEST_COPY_KEY)
		sc.DeleteBucket(TestBucket)
	}
	delFn(svc)
	defer delFn(svc)
	err := svc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	uploadId, err := svc.CreateMultiPartUpload(TestBucket, TestKey, s3.ObjectStorageClassStandard)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	partCount := 3
	completedUpload := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, partCount),
	}
	for i := 0; i < partCount; i++ {
		partNumber := int64(i + 1)
		etag, err := svc.UploadPart(TestBucket, TestKey, GenMinimalPart(), uploadId, partNumber)
		if err != nil {
			t.Fatal("UploadPart err:", err)
			panic(err)
		}
		completedUpload.Parts[i] = &s3.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int64(partNumber),
		}
	}
	err = svc.CompleteMultiPartUpload(TestBucket, TestKey, uploadId, completedUpload)
	if err != nil {
		t.Fatal("CompleteMultiPartUpload err:", err)
		err = svc.AbortMultiPartUpload(TestBucket, TestKey, uploadId)
		if err != nil {
			t.Fatal("AbortMultiPartUpload err:", err)
		}
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

	//clean up
	svc.DeleteObject(TestBucket, TestKey)
	svc.DeleteObject(TestBucket, TEST_COPY_KEY)
}

func Test_CopyObjectPartWithoutMD5(t *testing.T) {
	svc := NewS3WithoutMD5()
	TEST_COPY_KEY := "COPYED:" + TestKey
	delFn := func(sc *S3Client) {
		sc.DeleteObject(TestBucket, TestKey)
		sc.DeleteObject(TestBucket, TEST_COPY_KEY)
		sc.DeleteBucket(TestBucket)
	}
	delFn(svc)
	defer delFn(svc)
	err := svc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	uploadId, err := svc.CreateMultiPartUpload(TestBucket, TestKey, s3.ObjectStorageClassStandard)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	partCount := 3
	completedUpload := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, partCount),
	}

	for i := 0; i < partCount; i++ {
		partNumber := int64(i + 1)
		etag, err := svc.UploadPart(TestBucket, TestKey, GenMinimalPart(), uploadId, partNumber)
		if err != nil {
			t.Fatal("UploadPart err:", err)
			panic(err)
		}
		completedUpload.Parts[i] = &s3.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int64(partNumber),
		}
	}

	err = svc.CompleteMultiPartUpload(TestBucket, TestKey, uploadId, completedUpload)
	if err != nil {
		t.Fatal("CompleteMultiPartUpload err:", err)
		err = svc.AbortMultiPartUpload(TestBucket, TestKey, uploadId)
		if err != nil {
			t.Fatal("AbortMultiPartUpload err:", err)
		}
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

	//clean up
	svc.DeleteObject(TestBucket, TestKey)
	svc.DeleteObject(TestBucket, TEST_COPY_KEY)
}
