package lib

import (
	"errors"

	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
)

func (s3client *S3Client) PutBucketVersion(bucketName string, status string) error {
	params := &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String(status),
		},
	}
	_, err := s3client.Client.PutBucketVersioning(params)
	if err != nil {
		return err
	}
	return nil
}

func (s3client *S3Client) GetBucketVersion(bucketName string) (string, error) {
	params := &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucketName),
	}
	out, err := s3client.Client.GetBucketVersioning(params)
	if err != nil {
		return "", err
	}
	if out.Status == nil {
		return "", nil
	}
	if *out.Status != s3.BucketVersioningStatusEnabled && *out.Status != s3.BucketVersioningStatusSuspended {
		return "", errors.New("Invalid bucket versioning status: " + *out.Status)
	}
	return *out.Status, nil
}

func (s3client *S3Client) GetObjectVersionOutPut(bucketName, key, versionId string) (out *s3.GetObjectOutput, err error) {
	params := &s3.GetObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: aws.String(versionId),
	}
	return s3client.Client.GetObject(params)
}

func (s3client *S3Client) ListObjectVersions(bucketName, keyMarker, verIdMarker, prefix string, maxKey int64) (*s3.ListObjectVersionsOutput, error) {
	var vMarker *string
	if verIdMarker == "" {
		vMarker = nil
	} else {
		vMarker = aws.String(verIdMarker)
	}
	params := &s3.ListObjectVersionsInput{
		Bucket:          aws.String(bucketName),
		Delimiter:       aws.String("/"),
		KeyMarker:       aws.String(keyMarker),
		VersionIdMarker: vMarker,
		Prefix:          aws.String(prefix),
		MaxKeys:         aws.Int64(maxKey),
	}
	return s3client.Client.ListObjectVersions(params)
}

func (s3client *S3Client) DeleteObjectVersion(bucketName, objectName, version string) (*s3.DeleteObjectOutput, error) {
	params := &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectName),
		VersionId: aws.String(version),
	}
	return s3client.Client.DeleteObject(params)
}
