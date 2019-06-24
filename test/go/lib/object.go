package lib

import (
	"bytes"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"io/ioutil"
	"time"
	"github.com/journeymidnight/yig/meta/types"
)

func (s3client *S3Client) PutObject(bucketName, key, value string) (err error) {
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(value)),
	}
	if _, err = s3client.Client.PutObject(params); err != nil {
		return err
	}
	return
}

func (s3client *S3Client) PutObjectPreSignedWithSpecifiedBody(bucketName, key, value string, expire time.Duration) (url string, err error) {
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(value)),
	}
	req, _ := s3client.Client.PutObjectRequest(params)
	return req.Presign(expire)
}

func (s3client *S3Client) PutObjectPreSignedWithoutSpecifiedBody(bucketName, key, value string, expire time.Duration) (url string, err error) {
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	req, _ := s3client.Client.PutObjectRequest(params)
	return req.Presign(expire)
}

func (s3client *S3Client) HeadObject(bucketName, key string) (err error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	_, err = s3client.Client.HeadObject(params)
	if err != nil {
		return err
	}
	return
}

func (s3client *S3Client) GetObject(bucketName, key string) (value string, err error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	out, err := s3client.Client.GetObject(params)
	if err != nil {
		return "", err
	}
	data, err := ioutil.ReadAll(out.Body)
	return string(data), err
}

func (s3client *S3Client) GetObjectOutPut(bucketName, key string) (out *s3.GetObjectOutput, err error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	return s3client.Client.GetObject(params)
}

func (s3client *S3Client) GetObjectPreSigned(bucketName, key string, expire time.Duration) (url string, err error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	req, _ := s3client.Client.GetObjectRequest(params)
	return req.Presign(expire)
}

func (s3client *S3Client) DeleteObject(bucketName, key string) (err error) {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	_, err = s3client.Client.DeleteObject(params)
	if err != nil {
		return err
	}
	return
}

func (s3client *S3Client) AppendObject(bucketName, key, value string, position int64) (nextPos int64, err error) {
	var out *s3.AppendObjectOutput
	params := &s3.AppendObjectInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(key),
		Body:     bytes.NewReader([]byte(value)),
		Position: aws.Int64(position),
	}
	if out, err = s3client.Client.AppendObject(params); err != nil {
		return 0, err
	}

	return *out.NextPosition, nil
}

func (s3client *S3Client) PutObjectWithStorageClass(bucketName, key, value string, storageClass string) (err error) {
	params := &s3.PutObjectInput{
		Bucket:       aws.String(bucketName),
		Key:          aws.String(key),
		Body:         bytes.NewReader([]byte(value)),
		StorageClass: aws.String(storageClass),
	}
	if _, err = s3client.Client.PutObject(params); err != nil {
		return err
	}
	return
}


func (s3client *S3Client) ChangeObjectStorageClass(bucketName, key string, storageClass string) (err error) {
	params := &s3.CopyObjectInput{
		Bucket:            aws.String(bucketName),
		Key:               aws.String(key),
		CopySource:        aws.String("/" + bucketName + "/" + key),
		MetadataDirective: aws.String("REPLACE"),
		StorageClass:      aws.String(storageClass),
	}
	if _, err = s3client.Client.CopyObject(params); err != nil {
		return err
	}
	return
}
