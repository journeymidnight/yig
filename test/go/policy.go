package _go

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func (s3client *S3Client) PutBucketPolicy(bucketName, policy string) (err error) {
	params := &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucketName),
		Policy: aws.String(policy),
	}
	_, err = s3client.Client.PutBucketPolicy(params)
	if err != nil {
		return
	}
	return
}

func (s3client *S3Client) GetBucketPolicy(bucketName string) (policy string, err error) {
	params := &s3.GetBucketPolicyInput{
		Bucket: aws.String(bucketName),
	}
	out, err := s3client.Client.GetBucketPolicy(params)
	if err != nil {
		return "", err
	}
	return *out.Policy, err
}

func (s3client *S3Client) DeleteBucketPolicy(bucketName string) (err error) {
	params := &s3.DeleteBucketPolicyInput{
		Bucket: aws.String(bucketName),
	}
	_, err = s3client.Client.DeleteBucketPolicy(params)
	if err != nil {
		return
	}
	return
}
