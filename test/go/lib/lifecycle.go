package lib

import (
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
)

func (s3client *S3Client) PutBucketLifecycle(bucketName string, config *s3.BucketLifecycleConfiguration) (err error) {
	params := &s3.PutBucketLifecycleConfigurationInput{
		Bucket:                 aws.String(bucketName),
		LifecycleConfiguration: config,
	}
	_, err = s3client.Client.PutBucketLifecycleConfiguration(params)
	return err
}

func (s3client *S3Client) GetBucketLifecycle(bucketName string) (ret string, err error) {
	params := &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucketName),
	}
	out, err := s3client.Client.GetBucketLifecycleConfiguration(params)
	return out.String(), err
}

func (s3client *S3Client) DeleteBucketLifecycle(bucketName string) (ret string, err error) {
	params := &s3.DeleteBucketLifecycleInput{
		Bucket: aws.String(bucketName),
	}
	out, err := s3client.Client.DeleteBucketLifecycle(params)
	return out.String(), err
}
