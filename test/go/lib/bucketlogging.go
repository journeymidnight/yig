package lib

import (
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
)

func (s3client *S3Client) PutBucketLogging(bucketName string, rules *s3.LoggingEnabled) (err error) {
	params := &s3.PutBucketLoggingInput{
		Bucket:              aws.String(bucketName),
		BucketLoggingStatus: &s3.BucketLoggingStatus{LoggingEnabled: rules},
	}
	if _, err = s3client.Client.PutBucketLogging(params); err != nil {
		return err
	}
	return
}

func (s3client *S3Client) GetBucketLogging(bucketName string) (rules *s3.GetBucketLoggingOutput, err error) {
	params := &s3.GetBucketLoggingInput{
		Bucket: aws.String(bucketName),
	}
	rules, err = s3client.Client.GetBucketLogging(params)
	if err != nil {
		return nil, err
	}
	return rules, nil
}
