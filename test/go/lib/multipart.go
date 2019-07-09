package lib

import (
	"bytes"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
)

func (s3client *S3Client) CreateMultiPartUpload(bucketName, key, storageClass string) (uploadId string, err error) {
	params := &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(bucketName),
		Key:          aws.String(key),
		StorageClass: aws.String(storageClass),
	}
	out, err := s3client.Client.CreateMultipartUpload(params)
	if err != nil {
		return
	}
	return *out.UploadId, nil
}

func (s3client *S3Client) UploadPart(bucketName, key string, value []byte, uploadId string, partNumber int64) (etag string, err error) {
	params := &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(key),
		Body:       bytes.NewReader(value),
		PartNumber: aws.Int64(partNumber),
		UploadId:   aws.String(uploadId),
	}
	out, err := s3client.Client.UploadPart(params)
	if err != nil {
		return
	}
	return *out.ETag, nil
}

func (s3client *S3Client) CompleteMultiPartUpload(bucketName, key, uploadId string, completed *s3.CompletedMultipartUpload) (err error) {
	params := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String(key),
		MultipartUpload: completed,
		UploadId:        aws.String(uploadId),
	}
	if _, err = s3client.Client.CompleteMultipartUpload(params); err != nil {
		return err
	}
	return
}

func (s3client *S3Client) AbortMultiPartUpload(bucketName, key, uploadId string) (err error) {
	params := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
	}
	_, err = s3client.Client.AbortMultipartUpload(params)
	return
}
