package _go

import (
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	. "github.com/journeymidnight/yig/test/go/lib"
	"testing"
)

const (
	TEST_PUT_VALUE1 = "```[default]access_key = hehehehesecret_key = hehehehedefault_mime_type = binary/octet-streamenable_multipart = Trueencoding = UTF-8encrypt = Falseuse_https = Falsehost_base =s3.test.com:8080host_bucket = %(bucket)s.s3.test.com:8080multipart_chunk_size_mb = 128```"
	TEST_PUT_VALUE2 = "At its core, Yig extend minio backend storage to allow more than one ceph cluster work together and form a super large storage resource pool, users could easily enlarge the pool`s capacity to EB level by adding a new ceph cluser to this pool. Benifits are avoiding data movement and IO drop down caused by adding new host or disks to old ceph cluster as usual way. To accomplish this goal, Yig need a distribute database to store meta infomation. Now already Support Tidb,MySql."
)

type PutRewriteTestUnits struct {
	Bucket string
	units  []Unit
}

type Unit struct {
	Objects PutObjectInput
	Fn      func(t *testing.T, input PutObjectInput, sc *S3Client) (err error)
}

type PutObjectInput struct {
	Bucket string
	Key    string
	Values []string
}

var testRewriteUnits = PutRewriteTestUnits{
	Bucket: TEST_BUCKET,
	units: []Unit{
		{
			Objects: PutObjectInput{
				Bucket: TEST_BUCKET,
				Key:    TEST_KEY,
				Values: []string{TEST_PUT_VALUE1, TEST_PUT_VALUE2},
			},
			Fn: doPutWithPut,
		},
		{
			Objects: PutObjectInput{
				Bucket: TEST_BUCKET,
				Key:    TEST_KEY,
				Values: []string{TEST_PUT_VALUE1},
			},
			Fn: doPutWithMulti,
		},
		{
			Objects: PutObjectInput{
				Bucket: TEST_BUCKET,
				Key:    TEST_KEY,
				Values: []string{TEST_PUT_VALUE1},
			},
			Fn: doMultiWithPut,
		},
		{
			Objects: PutObjectInput{
				Bucket: TEST_BUCKET,
				Key:    TEST_KEY,
			},
			Fn: doMultiWithMulti,
		},
	},
}

func doPutWithPut(t *testing.T, input PutObjectInput, sc *S3Client) (err error) {
	defer sc.DeleteObject(input.Bucket, input.Key)
	err = sc.PutObject(input.Bucket, input.Key, input.Values[0])
	if err != nil {
		return err
	}

	err = sc.PutObject(input.Bucket, input.Key, input.Values[1])
	if err != nil {
		return err
	}

	v, err := sc.GetObject(input.Bucket, input.Key)
	if err != nil {
		return err
	}
	if v != input.Values[1] {
		return err
	}
	t.Log("doPutWithPut: GetObject Success")
	return sc.DeleteObject(input.Bucket, input.Key)
}

func doPutWithMulti(t *testing.T, input PutObjectInput, sc *S3Client) (err error) {
	defer sc.DeleteObject(input.Bucket, input.Key)
	err = sc.PutObject(input.Bucket, input.Key, input.Values[0])
	if err != nil {
		return err
	}

	uploadId, err := sc.CreateMultiPartUpload(input.Bucket, input.Key, s3.ObjectStorageClassStandard)
	if err != nil {
		return err
	}

	partCount := 2
	completedUpload := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, partCount),
	}

	var data []byte
	for i := 0; i < partCount; i++ {
		partNumber := int64(i + 1)
		g := GenMinimalPart()
		data = append(data, g...)
		etag, err := sc.UploadPart(input.Bucket, input.Key, g, uploadId, partNumber)
		if err != nil {
			return err
		}
		completedUpload.Parts[i] = &s3.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int64(partNumber),
		}
	}

	err = sc.CompleteMultiPartUpload(input.Bucket, input.Key, uploadId, completedUpload)
	if err != nil {
		err = sc.AbortMultiPartUpload(input.Bucket, input.Key, uploadId)
		if err != nil {
			t.Fatal("doPutWithMulti: AbortMultiPartUpload err =", err)
		}
		return err
	}

	v, err := sc.GetObject(input.Bucket, input.Key)
	if err != nil {
		return err
	}
	if v != string(data) {
		t.Error("The result is the same")
	}
	t.Log("doPutWithMulti: GetObject Success")
	return sc.DeleteObject(input.Bucket, input.Key)
}

func doMultiWithPut(t *testing.T, input PutObjectInput, sc *S3Client) (err error) {
	defer sc.DeleteObject(input.Bucket, input.Key)
	uploadId, err := sc.CreateMultiPartUpload(input.Bucket, input.Key, s3.ObjectStorageClassStandard)
	if err != nil {
		return err
	}

	partCount := 3
	completedUpload := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, partCount),
	}

	for i := 0; i < partCount; i++ {
		partNumber := int64(i + 1)
		etag, err := sc.UploadPart(input.Bucket, input.Key, GenMinimalPart(), uploadId, partNumber)
		if err != nil {
			return err
		}
		completedUpload.Parts[i] = &s3.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int64(partNumber),
		}
	}

	err = sc.CompleteMultiPartUpload(input.Bucket, input.Key, uploadId, completedUpload)
	if err != nil {
		err = sc.AbortMultiPartUpload(input.Bucket, input.Key, uploadId)
		if err != nil {
			t.Fatal("doMultiWithPut: AbortMultiPartUpload err =", err)
		}
		return err
	}

	err = sc.PutObject(input.Bucket, input.Key, input.Values[0])
	if err != nil {
		return err
	}

	v, err := sc.GetObject(input.Bucket, input.Key)
	if err != nil {
		return err
	}
	if v != input.Values[0] {
		t.Error("The result is the same", v)
	}
	t.Log("doMultiWithPut: GetObject Success")
	return sc.DeleteObject(input.Bucket, input.Key)
}

func doMultiWithMulti(t *testing.T, input PutObjectInput, sc *S3Client) (err error) {
	defer sc.DeleteObject(input.Bucket, input.Key)
	uploadId1, err := sc.CreateMultiPartUpload(input.Bucket, input.Key, s3.ObjectStorageClassStandard)
	if err != nil {
		return err
	}

	partCount1 := 3
	completedUpload1 := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, partCount1),
	}

	for i := 0; i < partCount1; i++ {
		partNumber1 := int64(i + 1)
		etag1, err := sc.UploadPart(input.Bucket, input.Key, GenMinimalPart(), uploadId1, partNumber1)
		if err != nil {
			return err
		}
		completedUpload1.Parts[i] = &s3.CompletedPart{
			ETag:       aws.String(etag1),
			PartNumber: aws.Int64(partNumber1),
		}
	}

	err = sc.CompleteMultiPartUpload(input.Bucket, input.Key, uploadId1, completedUpload1)
	if err != nil {
		err = sc.AbortMultiPartUpload(input.Bucket, input.Key, uploadId1)
		if err != nil {
			t.Fatal("doMultiWithMulti: AbortMultiPartUpload err =", err)
		}
		return err
	}

	v1, err := sc.GetObject(input.Bucket, input.Key)
	if err != nil {
		return err
	}

	uploadId2, err := sc.CreateMultiPartUpload(input.Bucket, input.Key, s3.ObjectStorageClassStandard)
	if err != nil {
		return err
	}

	partCount2 := 3
	completedUpload2 := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, partCount2),
	}

	for i := 0; i < partCount2; i++ {
		partNumber2 := int64(i + 1)
		etag2, err := sc.UploadPart(input.Bucket, input.Key, GenMinimalPart(), uploadId2, partNumber2)
		if err != nil {
			return err
		}
		completedUpload2.Parts[i] = &s3.CompletedPart{
			ETag:       aws.String(etag2),
			PartNumber: aws.Int64(partNumber2),
		}
	}

	err = sc.CompleteMultiPartUpload(input.Bucket, input.Key, uploadId2, completedUpload2)
	if err != nil {
		err = sc.AbortMultiPartUpload(input.Bucket, input.Key, uploadId2)
		if err != nil {
			t.Fatal("doMultiWithMulti: AbortMultiPartUpload err =", err)
		}
		return err
	}

	v2, err := sc.GetObject(input.Bucket, input.Key)
	if err != nil {
		return err
	}

	if v1 == v2 {
		t.Error("The result is the same")
	}

	t.Log("doMultiWithMulti: GetObject Success")
	return sc.DeleteObject(input.Bucket, input.Key)
}

func Test_PutRewrite(t *testing.T) {
	Units := testRewriteUnits
	sc := NewS3Internal()
	defer sc.DeleteBucket(Units.Bucket)
	err := sc.MakeBucket(Units.Bucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
	}
	t.Log("MakeBucket Success.")
	for _, unit := range Units.units {
		err = unit.Fn(t, unit.Objects, sc)
		if err != nil {
			t.Error(err)
		}
	}
}
