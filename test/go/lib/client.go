package lib

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Client struct {
	Client *s3.S3
}

func NewS3() *S3Client {
	creds := credentials.NewStaticCredentials("hehehehe", "hehehehe", "")

	// By default make sure a region is specified
	s3client := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String("s3.test.com"),
			Region:      aws.String("r"),
		},
	),
	),
	)
	return &S3Client{s3client}
}
