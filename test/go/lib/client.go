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
	Endpoint         = "s3.test.com:8080"
	EndpointInternal = "s3-internal.test.com:8080"
	AccessKey        = "hehehehe"
	SecretKey        = "hehehehe"
	AccessKeyH       = "hahahaha"
	SecretKeyH       = "hahahaha"
	Region           = "RegionHeHe"

	TestBucket           = "mybucket"
	TestCopyBucket       = TestBucket + "copy"
	TestLifecycleBucket1 = TestBucket + "1"
	TestLifecycleBucket2 = TestBucket + "2"
	TestKey              = "testput"
	TestKeySpecial       = "testputspecial:!@$%^&*()_+=-;?><| "
	TestValue            = "valueput"
	TestIllegalReferer   = "http://www.thief.com/"
	TestLegalReferer     = "http://www.genltemen.com/"
	TestCommonReferer    = "http://www.common.com/"
)

func NewS3WithoutMD5() *S3Client {
	creds := credentials.NewStaticCredentials(AccessKey, SecretKey, "")

	// By default make sure a region is specified
	s3client := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials:                   creds,
			DisableSSL:                    aws.Bool(true),
			Endpoint:                      aws.String(Endpoint),
			Region:                        aws.String("r"),
			S3DisableContentMD5Validation: aws.Bool(true),
		},
	),
	),
	)
	return &S3Client{s3client}
}

func NewS3() *S3Client {
	creds := credentials.NewStaticCredentials(AccessKey, SecretKey, "")

	// By default make sure a region is specified
	s3client := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String(Endpoint),
			Region:      aws.String(Region),
		},
	),
	),
	)
	return &S3Client{s3client}
}

func NewS3Internal() *S3Client {
	creds := credentials.NewStaticCredentials(AccessKey, SecretKey, "")

	// By default make sure a region is specified
	s3client := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String(EndpointInternal),
			Region:      aws.String(Region),
		},
	),
	),
	)
	return &S3Client{s3client}
}

func NewS3Ha() *S3Client {
	creds := credentials.NewStaticCredentials(AccessKeyH, SecretKeyH, "")

	// By default make sure a region is specified
	s3client := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String(Endpoint),
			Region:      aws.String(Region),
		},
	),
	),
	)
	return &S3Client{s3client}
}
