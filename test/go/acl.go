package _go

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func (s3client *S3Client) GetBucketAcl(bucketName string) (ret string, err error) {
	params := &s3.GetBucketAclInput{
		Bucket: aws.String(bucketName),
	}
	out, err := s3client.Client.GetBucketAcl(params)
	return out.String(), err
}
