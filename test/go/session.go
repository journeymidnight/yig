package _go

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func SessionNew(config *Config) *s3.S3 {
	creds := credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, "")

	// By default make sure a region is specified
	return s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String(config.EndPoint),
			Region:      aws.String("r"),
		},
	),
	),
	)
}

func SessionForBucket(svc *s3.S3, bucket string) (*s3.S3, error) {
	if loc, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: &bucket}); err != nil {
		return nil, err
	} else if loc.LocationConstraint != nil {
		return s3.New(session.Must(session.NewSession(&svc.Client.Config, &aws.Config{Region: loc.LocationConstraint}))), nil
	}
	return svc, nil
}
