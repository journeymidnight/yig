package main

import (
	"fmt"
	"testing"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"bytes"
	"time"
	"strconv"
)

var bucketPrefix = "mytest-"
var endpoint = "s3.test.com:3000"
var disableSSL = true
var ak = "hehehe"
var sk = "hehehe"
var region = "us-east-1"

func newSession() (*session.Session, error){
	creds := credentials.NewStaticCredentials(ak, sk, "")
	config := &aws.Config{
		Region:           aws.String(region),
		Endpoint:         &endpoint,
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      creds,
		DisableSSL:       &disableSSL,
	}
	return session.NewSession(config)
}

func putObject(bucket, object, content string) (err error) {

	sess, err := newSession()
	if err != nil {
		fmt.Println("failed to create session,", err)
		return
	}
	svc := s3.New(sess)

	params := &s3.PutObjectInput{
		Bucket:             aws.String(bucket), // Required
		Key:                aws.String(object),  // Required
		Body:               bytes.NewReader([]byte(content)),
	}
	_, err = svc.PutObject(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		return
	}
	return
}


func createBucket(bucket string) (err error) {

	sess, err := newSession()
	if err != nil {
		fmt.Println("failed to create session,", err)
		return
	}
	svc := s3.New(sess)

	params := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err = svc.CreateBucket(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		return
	}
	return
}

func validate_bucket_list(t *testing.T, bucket, prefix, delimiter, marker string, maxKeys int, truncated bool,
                      checkObjs []string, checkPrefixes []string, nextMarker string) (retMarker string, err error) {

	sess, err := newSession()
	if err != nil {
		fmt.Println("failed to create session,", err)
		return
	}
	svc := s3.New(sess)

	input := &s3.ListObjectsInput{
		Bucket:                 aws.String(bucket),
		Delimiter:              aws.String(delimiter),
		Prefix:                 aws.String(prefix),
		MaxKeys:                aws.Int64(int64(maxKeys)),
		Marker:                 aws.String(marker),
	}
	output, err := svc.ListObjects(input)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if *output.IsTruncated != truncated {
		err = errors.New("wrong truncated!")
		return
	}

	var wrongMarker bool
	if output.NextMarker != nil && nextMarker != "" {
		if *output.NextMarker != nextMarker {
			wrongMarker = true
		}
	} else if output.NextMarker == nil && nextMarker == "" {
		wrongMarker = false
	} else {
		wrongMarker = true
	}
	if wrongMarker {
		fmt.Println("output.NextMarker:", *output.NextMarker, "nextMarker", nextMarker)
		err = errors.New("wrong nextMarker!")
		return
	}

	if len(output.Contents) != len(checkObjs) {
		err = errors.New("wrong sum of total objects!")
		return
	}

	if len(output.CommonPrefixes) != len(checkPrefixes) {
		err = errors.New("wrong sum of total commonPrefix!")
		return
	}

	for i, obj := range output.Contents {
		if *obj.Key != checkObjs[i] {
			err = errors.New("wrong object!")
			return
		}
	}

	for i, obj := range output.CommonPrefixes {
		if *obj.Prefix != checkPrefixes[i] {
			err = errors.New("wrong commonPrefix!")
			return
		}
	}

	return nextMarker, nil

}

func Test_ListObjects(t *testing.T) {
	// get a bucket name
	now := time.Now()
	nowNum := now.Unix()
	bucket := bucketPrefix + strconv.Itoa(int(nowNum))

	err := createBucket(bucket)
	if err != nil {
		return
	}

	err = putObject(bucket, "asdf", "asdf")
	if err != nil {
		return
	}
	err = putObject(bucket, "boo/bar", "boo/bar")
	if err != nil {
		return
	}
	err = putObject(bucket, "boo/baz/xyzzy", "boo/baz/xyzzy")
	if err != nil {
		return
	}
	err = putObject(bucket, "cquux/thud", "cquux/thud")
	if err != nil {
		return
	}
	err = putObject(bucket, "cquux/bla", "cquux/bla")
	if err != nil {
		return
	}

	delim := "/"
	marker := ""
	prefix := ""

	marker, err = validate_bucket_list(t, bucket, prefix, delim, "", 1, true, []string{"asdf"}, nil, "asdf")
	if err != nil {
		t.Error(err.Error())
		return
	}
	marker, err = validate_bucket_list(t, bucket, prefix, delim, marker, 1, true, nil, []string{"boo/"}, "boo/")
	if err != nil {
		t.Error(err.Error())
		return
	}
	marker, err = validate_bucket_list(t, bucket, prefix, delim, marker, 1, false, nil, []string{"cquux/"}, "")
	if err != nil {
	        t.Error(err.Error())
		return
	}

	marker, err = validate_bucket_list(t, bucket, prefix, delim, "", 2, true, []string{"asdf"}, []string{"boo/"}, "boo/")
	if err != nil {
	        t.Error(err.Error())
		return
	}
	marker, err = validate_bucket_list(t, bucket, prefix, delim, marker, 2, false, nil, []string{"cquux/"}, "")
	if err != nil {
	        t.Error(err.Error())
		return
	}

	prefix = "boo/"

	marker, err = validate_bucket_list(t, bucket, prefix, delim, "", 1, true, []string{"boo/bar"}, nil, "boo/bar")
	if err != nil {
	        t.Error(err.Error())
		return
	}
	marker, err = validate_bucket_list(t, bucket, prefix, delim, marker, 1, false, nil, []string{"boo/baz/"}, "")
	if err != nil {
	        t.Error(err.Error())
		return
	}

	marker, err = validate_bucket_list(t, bucket, prefix, delim, "", 2, false, []string{"boo/bar"}, []string{"boo/baz/"}, "")
	if err != nil {
	        t.Error(err.Error())
		return
	}

	return
}
