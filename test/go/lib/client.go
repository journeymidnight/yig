package lib

import (
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/aws/credentials"
	"github.com/journeymidnight/aws-sdk-go/aws/session"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
)

type S3Client struct {
	Client *s3.S3
}

const (
	TEST_BUCKET = "mybucket"
	TEST_KEY    = "testput"
	TEST_KEY_SPECIAL = "testputspecial:!@$%^&*()_+=-;?><| "
	TEST_VALUE  = "valueput"
	TEST_ILLEGALREFERER = "http://www.thief.com/"
	TEST_LEGALREFERER = "http://www.genltemen.com/"
	TEST_COMMONREFERER = "http://www.common.com/"
)

func NewS3WithoutMD5() *S3Client {
	creds := credentials.NewStaticCredentials("hehehehe", "hehehehe", "")

	// By default make sure a region is specified
	s3client := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String("s3.test.com:8080"),
			Region:      aws.String("r"),
			S3DisableContentMD5Validation:	aws.Bool(true),
		},
	),
	),
	)
	return &S3Client{s3client}
}

func NewS3() *S3Client {
	creds := credentials.NewStaticCredentials("hehehehe", "hehehehe", "")

	// By default make sure a region is specified
	s3client := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String("s3.test.com:8080"),
			Region:      aws.String("r"),
		},
	),
	),
	)
	return &S3Client{s3client}
}

func NewS3Internal() *S3Client {
	creds := credentials.NewStaticCredentials("hehehehe", "hehehehe", "")

	// By default make sure a region is specified
	s3client := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String("s3-internal.test.com:8080"),
			Region:      aws.String("r"),
		},
	),
	),
	)
	return &S3Client{s3client}
}
