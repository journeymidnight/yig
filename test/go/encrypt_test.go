package _go

import (
	"encoding/xml"
	"testing"

	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/test/go/lib"
)

const (
	EncryptionSSES3XML = `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Rule>
			<ApplyServerSideEncryptionByDefault>
        		<SSEAlgorithm>AES256</SSEAlgorithm>
			</ApplyServerSideEncryptionByDefault>
		</Rule>
	</ServerSideEncryptionConfiguration>`

	EncryptionSSEKMSXML = `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Rule>
			<ApplyServerSideEncryptionByDefault>
        		<KMSMasterKeyID>arn:aws:kms:us-east-1:1234/5678example</KMSMasterKeyID>
			</ApplyServerSideEncryptionByDefault>
		</Rule>
	</ServerSideEncryptionConfiguration>`
)

func Test_PutBucketEncryption(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	defer func() {
		sc.DeleteBucket(TEST_BUCKET)
	}()

	var config = &datatype.EncryptionConfiguration{}
	err = xml.Unmarshal([]byte(EncryptionSSES3XML), config)
	if err != nil {
		t.Fatal("Unmarshal encryption configuration err:", err)
	}

	encryption := TransferToS3AccessEncryptionConfiguration(config)
	if encryption == nil {
		t.Fatal("PutBucketEncryption err:", "empty encryption!")
	}

	err = sc.PutBucketEncryptionWithXml(TEST_BUCKET, encryption)
	if err != nil {
		t.Fatal("PutBucketEncryptionWithXml err:", err)
	}
	t.Log("PutBucketEncryptionWithXml Success!")

	out, err := sc.GetBucketEncryption(TEST_BUCKET)
	if err != nil {
		t.Fatal("GetBucketEncryption err:", err)
	}
	t.Log("GetBucketEncryption Success! out:", out)

	out, err = sc.DeleteBucketEncryption(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucketEncryption err:", err)
	}
	t.Log("DeleteBucketEncryption Success! out:", out)
}

func Test_PutObejctWithSetBucketEncryption(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	defer func() {
		sc.DeleteObject(TEST_BUCKET, TEST_KEY)
		sc.DeleteBucket(TEST_BUCKET)
	}()
	var config = &datatype.EncryptionConfiguration{}
	err = xml.Unmarshal([]byte(EncryptionSSES3XML), config)
	if err != nil {
		t.Fatal("Unmarshal encryption configuration err:", err)
	}

	encryption := TransferToS3AccessEncryptionConfiguration(config)
	if encryption == nil {
		t.Fatal("PutBucketEncryption err:", "empty encryption!")
	}

	err = sc.PutBucketEncryptionWithXml(TEST_BUCKET, encryption)
	if err != nil {
		t.Fatal("PutBucketEncryptionWithXml err:", err)
	}
	t.Log("PutBucketEncryptionWithXml Success!")

	out, err := sc.GetBucketEncryption(TEST_BUCKET)
	if err != nil {
		t.Fatal("GetBucketEncryption err:", err)
	}
	t.Log("GetBucketEncryption Success! out:", out)

	//PUT object
	err = sc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	t.Log("PutObject Success!")

	out, err = sc.GetBucketEncryption(TEST_BUCKET)
	if err != nil {
		t.Fatal("GetBucketEncryption err:", err)
	}
	t.Log("GetBucketEncryption Success! out:", out)

	//GET object
	v, err := sc.GetEncryptObjectWithSSES3(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("GetEncryptObjectWithSSES3 err:", err)
	}
	if v != TEST_VALUE {
		t.Fatal("GetEncryptObjectWithSSES3 err: value is:", v, ", but should be:", TEST_VALUE)
	}
	t.Log("GetEncryptObjectWithSSES3 Success value:", v)

	out, err = sc.DeleteBucketEncryption(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucketEncryption err:", err)
	}
	t.Log("DeleteBucketEncryption Success! out:", out)
}

func Test_PutEncryptObjectWithSSEC(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	defer func() {
		sc.DeleteObject(TEST_BUCKET, TEST_KEY)
		sc.DeleteBucket(TEST_BUCKET)
	}()
	err = sc.PutEncryptObjectWithSSEC(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutEncryptObjectWithSSEC err:", err)
	}
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
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	defer func() {
		sc.DeleteObject(TEST_BUCKET, TEST_KEY)
		sc.DeleteBucket(TEST_BUCKET)
	}()
	err = sc.PutEncryptObjectWithSSES3(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutEncryptObjectWithSSES3 err:", err)
	}
	t.Log("PutEncryptObjectWithSSES3 Success!")
	v, err := sc.GetEncryptObjectWithSSES3(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("GetEncryptObjectWithSSES3 err:", err)
	}
	if v != TEST_VALUE {
		t.Fatal("GetEncryptObjectWithSSES3 err: value is:", v, ", but should be:", TEST_VALUE)
	}
	t.Log("GetEncryptObjectWithSSES3 Success value:", v)
}

func Test_CopyObjectSourceIsSSES3(t *testing.T) {
	svc := NewS3()
	defer func() {
		svc.DeleteObject(TEST_BUCKET, TEST_KEY)
		svc.DeleteObject(TEST_COPY_BUCKET, TEST_KEY)
		svc.DeleteBucket(TEST_BUCKET)
		svc.DeleteBucket(TEST_COPY_BUCKET)
	}()
	err := svc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	err = svc.MakeBucket(TEST_COPY_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	err = svc.PutEncryptObjectWithSSES3(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutEncryptObjectWithSSES3 err:", err)
	}
	t.Log("PutEncryptObjectWithSSES3 Success!")

	input := &s3.CopyObjectInput{
		Bucket:     aws.String(TEST_COPY_BUCKET),
		CopySource: aws.String(TEST_BUCKET + "/" + TEST_KEY),
		Key:        aws.String(TEST_KEY),
	}
	_, err = svc.Client.CopyObject(input)
	if err != nil {
		t.Fatal("Copy Object err:", err)
	}

	//verify them
	v1, err := svc.GetEncryptObjectWithSSES3(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	v2, err := svc.GetEncryptObjectWithSSES3(TEST_COPY_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	if v1 != v2 {
		t.Fatal("Copyed result is not the same.")
	}
}

func Test_CopyObjectWithSourceBucketEncryptionIsSSES3(t *testing.T) {
	svc := NewS3()

	err := svc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	err = svc.MakeBucket(TEST_COPY_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	//Set Bucket Encryption
	var config = &datatype.EncryptionConfiguration{}
	err = xml.Unmarshal([]byte(EncryptionSSES3XML), config)
	if err != nil {
		t.Fatal("Unmarshal encryption configuration err:", err)
	}
	encryption := TransferToS3AccessEncryptionConfiguration(config)
	if encryption == nil {
		t.Fatal("PutBucketEncryption err:", "empty encryption!")
	}
	err = svc.PutBucketEncryptionWithXml(TEST_BUCKET, encryption)
	if err != nil {
		t.Fatal("PutBucketEncryptionWithXml err:", err)
	}
	t.Log("PutBucketEncryptionWithXml Success!")
	out, err := svc.GetBucketEncryption(TEST_BUCKET)
	if err != nil {
		t.Fatal("GetBucketEncryption err:", err)
	}
	t.Log("GetBucketEncryption Success! out:", out)

	//PUT object
	err = svc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	t.Log("PutObject Success!")

	//Copy
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(TEST_COPY_BUCKET),
		CopySource: aws.String(TEST_BUCKET + "/" + TEST_KEY),
		Key:        aws.String(TEST_KEY),
	}
	_, err = svc.Client.CopyObject(input)
	if err != nil {
		t.Fatal("Copy Object err:", err)
	}
	t.Log("CopyObject Success!")

	//verify them
	v1, err := svc.GetEncryptObjectWithSSES3(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	v2, err := svc.GetEncryptObjectWithSSES3(TEST_COPY_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	if v1 != v2 {
		t.Fatal("Copyed result is not the same.")
	}

	//clean up
	err = svc.DeleteObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Log("DeleteObject err:", err)
	}
	err = svc.DeleteObject(TEST_COPY_BUCKET, TEST_KEY)
	if err != nil {
		t.Log("DeleteObject err:", err)
	}
	err = svc.DeleteBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
	err = svc.DeleteBucket(TEST_COPY_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
}

func Test_CopyObjectWithSetSSES3(t *testing.T) {
	svc := NewS3()

	err := svc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	err = svc.MakeBucket(TEST_COPY_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	//PUT object
	err = svc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	t.Log("PutObject Success!")

	//Copy
	input := &s3.CopyObjectInput{
		Bucket:               aws.String(TEST_COPY_BUCKET),
		CopySource:           aws.String(TEST_BUCKET + "/" + TEST_KEY),
		Key:                  aws.String(TEST_KEY),
		ServerSideEncryption: aws.String("AES256"),
	}
	_, err = svc.Client.CopyObject(input)
	if err != nil {
		t.Fatal("Copy Object err:", err)
	}
	t.Log("CopyObject Success!")

	//verify them
	v1, err := svc.GetEncryptObjectWithSSES3(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	v2, err := svc.GetEncryptObjectWithSSES3(TEST_COPY_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	if v1 != v2 {
		t.Fatal("Copyed result is not the same.")
	}

	//clean up
	err = svc.DeleteObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Log("DeleteObject err:", err)
	}
	err = svc.DeleteObject(TEST_COPY_BUCKET, TEST_KEY)
	if err != nil {
		t.Log("DeleteObject err:", err)
	}
	err = svc.DeleteBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
	err = svc.DeleteBucket(TEST_COPY_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
}

func Test_CopyObjectWithTargetBucketEncryptionIsSSES3(t *testing.T) {
	svc := NewS3()

	err := svc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	err = svc.MakeBucket(TEST_COPY_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	//Set Bucket Encryption
	var config = &datatype.EncryptionConfiguration{}
	err = xml.Unmarshal([]byte(EncryptionSSES3XML), config)
	if err != nil {
		t.Fatal("Unmarshal encryption configuration err:", err)
	}
	encryption := TransferToS3AccessEncryptionConfiguration(config)
	if encryption == nil {
		t.Fatal("PutBucketEncryption err:", "empty encryption!")
	}
	err = svc.PutBucketEncryptionWithXml(TEST_COPY_BUCKET, encryption)
	if err != nil {
		t.Fatal("PutBucketEncryptionWithXml err:", err)
	}
	t.Log("PutBucketEncryptionWithXml Success!")
	out, err := svc.GetBucketEncryption(TEST_BUCKET)
	if err != nil {
		t.Fatal("GetBucketEncryption err:", err)
	}
	t.Log("GetBucketEncryption Success! mybucket out:", out)
	out, err = svc.GetBucketEncryption(TEST_COPY_BUCKET)
	if err != nil {
		t.Fatal("GetBucketEncryption err:", err)
	}
	t.Log("GetBucketEncryption Success! mycopybucket out:", out)

	//PUT object
	err = svc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	t.Log("PutObject Success!")

	//Copy
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(TEST_COPY_BUCKET),
		CopySource: aws.String(TEST_BUCKET + "/" + TEST_KEY),
		Key:        aws.String(TEST_KEY),
	}
	_, err = svc.Client.CopyObject(input)
	if err != nil {
		t.Fatal("Copy Object err:", err)
	}
	t.Log("CopyObject Success!")

	//verify them
	v1, err := svc.GetEncryptObjectWithSSES3(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	v2, err := svc.GetEncryptObjectWithSSES3(TEST_COPY_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	if v1 != v2 {
		t.Fatal("Copyed result is not the same.")
	}

	//clean up
	err = svc.DeleteObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Log("DeleteObject err:", err)
	}
	err = svc.DeleteObject(TEST_COPY_BUCKET, TEST_KEY)
	if err != nil {
		t.Log("DeleteObject err:", err)
	}
	err = svc.DeleteBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
	err = svc.DeleteBucket(TEST_COPY_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
}

func Test_MultipartUploadWithSSES3(t *testing.T) {
	sc := NewS3()
	defer sc.CleanEnv()
	sc.CleanEnv()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	uploadId, err := sc.CreateMultiPartUploadWithSSES3(TEST_BUCKET, TEST_KEY, s3.ObjectStorageClassStandard)
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

	_, err = sc.GetEncryptObjectWithSSES3(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("GetEncryptObjectWithSSES3 err:", err)
	}
	t.Log("GetEncryptObjectWithSSES3 Success value:")
}

func Test_CopyObjectPartWithSSES3(t *testing.T) {
	svc := NewS3()
	err := svc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	err = svc.MakeBucket(TEST_COPY_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	//upload
	uploadId, err := svc.CreateMultiPartUploadWithSSES3(TEST_BUCKET, TEST_KEY, s3.ObjectStorageClassStandard)
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

	//Copy
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(TEST_COPY_BUCKET),
		CopySource: aws.String(TEST_BUCKET + "/" + TEST_KEY),
		Key:        aws.String(TEST_KEY),
	}
	_, err = svc.Client.CopyObject(input)
	if err != nil {
		t.Fatal("Copy Object err:", err)
	}
	t.Log("CopyObject Success!")

	//verify them
	v1, err := svc.GetEncryptObjectWithSSES3(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	v2, err := svc.GetEncryptObjectWithSSES3(TEST_COPY_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("Get Object err:", err)
	}
	if v1 != v2 {
		t.Fatal("Copyed result is not the same.")
	}

	//clean up
	err = svc.DeleteObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Log("DeleteObject err:", err)
	}
	err = svc.DeleteObject(TEST_COPY_BUCKET, TEST_KEY)
	if err != nil {
		t.Log("DeleteObject err:", err)
	}
	err = svc.DeleteBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
	err = svc.DeleteBucket(TEST_COPY_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
}
