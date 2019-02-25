package lib

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"io/ioutil"
)

func (s3client *S3Client) PutEncryptObjectWithSSEC(bucketName, key, value string) (err error) {
	ssekey := "qwertyuiopasdfghjklzxcvbnmaaaaaa"
	hash := md5.New()
	hash.Write([]byte(ssekey))
	sum := hash.Sum(nil)
	params := &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(key),
		Body:                 bytes.NewReader([]byte(value)),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(ssekey),
		SSECustomerKeyMD5:    aws.String(base64.StdEncoding.EncodeToString(sum)),
	}
	if _, err := s3client.Client.PutObject(params); err != nil {
		return err
	}
	return err
}

func (s3client *S3Client) GetEncryptObjectWithSSEC(bucketName, key string) (value string, err error) {
	ssekey := "qwertyuiopasdfghjklzxcvbnmaaaaaa"
	hash := md5.New()
	hash.Write([]byte(ssekey))
	sum := hash.Sum(nil)
	params := &s3.GetObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(key),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(ssekey),
		SSECustomerKeyMD5:    aws.String(base64.StdEncoding.EncodeToString(sum)),
	}
	out, err := s3client.Client.GetObject(params)
	if err != nil {
		return "", err
	}
	data, err := ioutil.ReadAll(out.Body)
	return string(data), err
}

func (s3client *S3Client) PutEncryptObjectWithSSES3(bucketName, key, value string) (err error) {
	params := &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(key),
		Body:                 bytes.NewReader([]byte(value)),
		ServerSideEncryption: aws.String("AES256"),
	}
	if _, err := s3client.Client.PutObject(params); err != nil {
		return err
	}
	return err
}

func (s3client *S3Client) GetEncryptObjectWithSSES3(bucketName, key string) (value string, err error) {
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
