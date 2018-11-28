package _go

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func (s3client *S3Client) PutEncryptObject(bucketName, key, value string) (err error) {
	params := &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(key),
		Body:                 bytes.NewReader([]byte(value)),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String("hehehehe"),
	}
	if _, err := s3client.Client.PutObject(params); err != nil {
		return err
	}
	return err
}
