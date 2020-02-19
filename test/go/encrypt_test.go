package _go

import (
	"encoding/xml"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/test/go/lib"
	"testing"
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

func Test_Encrypt_Prepare(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
}

func Test_PutBucketEncryption(t *testing.T) {
	sc := NewS3()

	var config = &datatype.EncryptionConfiguration{}
	err := xml.Unmarshal([]byte(EncryptionSSES3XML), config)
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

	var config = &datatype.EncryptionConfiguration{}
	err := xml.Unmarshal([]byte(EncryptionSSES3XML), config)
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
}

func Test_GetObjectWithBucketEncryption(t *testing.T) {
	sc := NewS3()

	out, err := sc.GetBucketEncryption(TEST_BUCKET)
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

func Test_CopyObjectSourceIsSSES3(t *testing.T) {
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
