package lib

import (
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
)

func (s3client *S3Client) PutBucketWebsite(bucketName, index, error string) (err error) {
	params := &s3.PutBucketWebsiteInput{
		Bucket: aws.String(bucketName),
		WebsiteConfiguration: &s3.WebsiteConfiguration{
			IndexDocument: &s3.IndexDocument{Suffix: aws.String(index)},
			ErrorDocument: &s3.ErrorDocument{Key: aws.String(error)},
		},
	}
	_, err = s3client.Client.PutBucketWebsite(params)
	if err != nil {
		return
	}
	return
}

func (s3client *S3Client) PutBucketWebsiteWithConf(bucketName string, conf *s3.WebsiteConfiguration) (err error) {
	params := &s3.PutBucketWebsiteInput{
		Bucket:               aws.String(bucketName),
		WebsiteConfiguration: conf,
	}
	_, err = s3client.Client.PutBucketWebsite(params)
	if err != nil {
		return
	}
	return
}

func (s3client *S3Client) GetBucketWebsite(bucketName string) (conf string, err error) {
	params := &s3.GetBucketWebsiteInput{
		Bucket: aws.String(bucketName),
	}
	out, err := s3client.Client.GetBucketWebsite(params)
	if err != nil {
		return
	}
	return out.String(), nil
}

func (s3client *S3Client) DeleteBucketWebsite(bucketName string) (err error) {
	params := &s3.DeleteBucketWebsiteInput{
		Bucket: aws.String(bucketName),
	}
	_, err = s3client.Client.DeleteBucketWebsite(params)
	if err != nil {
		return
	}
	return
}
