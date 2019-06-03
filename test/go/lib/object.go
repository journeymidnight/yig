package lib

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
)

func (s3client *S3Client) PutObject(bucketName, key, value string) (err error) {
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(value)),
	}
	if _, err = s3client.Client.PutObject(params); err != nil {
		return err
	}
	return
}

func (s3client *S3Client) PutObjectPreSignedWithSpecifiedBody(bucketName, key, value string, expire time.Duration) (url string, err error) {
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(value)),
	}
	req, _ := s3client.Client.PutObjectRequest(params)
	return req.Presign(expire)
}

func (s3client *S3Client) PutObjectPreSignedWithoutSpecifiedBody(bucketName, key, value string, expire time.Duration) (url string, err error) {
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	req, _ := s3client.Client.PutObjectRequest(params)
	return req.Presign(expire)
}

func (s3client *S3Client) HeadObject(bucketName, key string) (err error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	_, err = s3client.Client.HeadObject(params)
	if err != nil {
		return err
	}
	return
}

func (s3client *S3Client) GetObject(bucketName, key string) (value string, err error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	out, err := s3client.Client.GetObject(params)
	if err != nil {
		return "", err
	}
	data, err := ioutil.ReadAll(out.Body)
	return string(data), err
}

func (s3client *S3Client) GetObjectPreSigned(bucketName, key string, expire time.Duration) (url string, err error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	req, _ := s3client.Client.GetObjectRequest(params)
	return req.Presign(expire)
}

func (s3client *S3Client) DeleteObject(bucketName, key string) (err error) {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	_, err = s3client.Client.DeleteObject(params)
	if err != nil {
		return err
	}
	return
}

func (s3client *S3Client) AppendObject(bucketName, key, value string, position int64) (nextPos int64, err error) {
	var out *s3.AppendObjectOutput
	params := &s3.AppendObjectInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(key),
		Body:     bytes.NewReader([]byte(value)),
		Position: aws.Int64(position),
	}
	if out, err = s3client.Client.AppendObject(params); err != nil {
		return 0, err
	}

	return *out.NextPosition, nil
}

func (s3client *S3Client) CreateMultipartUpload(bucketName, key string) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: &bucketName,
		Key:    &key,
	}

	output, err := s3client.Client.CreateMultipartUpload(input)
	if err != nil {
		return "", err
	}
	if output.Bucket == nil || (*output.Bucket) != bucketName ||
		output.Key == nil || (*output.Key) != key {
		return "", errors.New(fmt.Sprintf("failed to create multipart upload, input bucket: %s, output bucket: %s, input key: %s, output key: %s", bucketName, *output.Bucket, key, *output.Key))
	}
	return *output.UploadId, nil
}

type UploadPartInput struct {
	UploadId   string
	BucketName string
	Key        string
	PartNum    int64
	Body       []byte
}

func (s3client *S3Client) UploadPart(upi *UploadPartInput) (string, error) {
	body := bytes.NewReader(upi.Body)
	input := &s3.UploadPartInput{
		Bucket:     &upi.BucketName,
		Key:        &upi.Key,
		PartNumber: &upi.PartNum,
		Body:       body,
		UploadId:   &upi.UploadId,
	}

	out, err := s3client.Client.UploadPart(input)
	if err != nil {
		return "", err
	}

	return *out.ETag, nil
}

func (s3client *S3Client) AbortMultipart(bucket, key, uploadId string) error {
	input := &s3.AbortMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: &uploadId,
	}

	_, err := s3client.Client.AbortMultipartUpload(input)
	return err
}

type PartInfo struct {
	PartNumber *int64
	ETag       *string
}

func (s3client *S3Client) CompleteMultipart(bucket, key, uploadId string, parts []PartInfo) (string, error) {
	var partList []*s3.CompletedPart
	for _, p := range parts {
		part := &s3.CompletedPart{
			ETag:       p.ETag,
			PartNumber: p.PartNumber,
		}
		partList = append(partList, part)
	}
	partInfo := &s3.CompletedMultipartUpload{
		Parts: partList,
	}

	input := &s3.CompleteMultipartUploadInput{
		Bucket:          &bucket,
		Key:             &key,
		UploadId:        &uploadId,
		MultipartUpload: partInfo,
	}

	out, err := s3client.Client.CompleteMultipartUpload(input)
	if err != nil {
		return "", err
	}
	return *out.ETag, nil
}
