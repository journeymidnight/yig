package lib

import (
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
)

const (
	GetObjectPolicy_1 = `{
			"Version": "2012-10-17",
			"Statement": [{
			"Effect": "Allow",
			"Principal": {"AWS":"*"},
			"Action": ["s3:GetObject"],
			"Resource": [
				"arn:aws:s3:::` + TestBucket + `/*"
			]
			}]
		}`

	GetObjectPolicy_2 = `{
			"Version": "2012-10-17",
			"Statement": [{
			"Effect": "Allow",
			"Principal": {"AWS":"*"},
			"Action": ["s3:GetObject"],
			"Resource": [
				"arn:aws:s3:::` + TestBucket + `/test/*"
			]
			}]
		}`

	SetBucketPolicyAllowStringLike = `{
			"Version": "2012-10-17",
			"Id": "http referer policy example",
			"Statement": [
				{
					"Sid": "Allow get requests referred by url test1",
					"Effect":"Allow",
					"Principal": {
						"AWS":"*"
					},
					"Action":["s3:GetObject"],
					"Resource":[
							"arn:aws:s3:::` + TestBucket + `",
							"arn:aws:s3:::` + TestBucket + `/*"
					],
					"Condition":
							{"StringLike":{"aws:Referer":["http://www.genltemen.com/*","http://genltemen.com/*"]}}
				}
			]
		}`

	SetBucketPolicyAllowStringNotLike = `{
			"Version": "2012-10-17",
			"Id": "http referer policy example",
			"Statement": [
				{
					"Sid": "Allow get requests referred by url test2",
					"Effect":"Allow",
					"Principal": {
						"AWS":"*"
					},
					"Action":["s3:GetObject"],
					"Resource":[
							"arn:aws:s3:::` + TestBucket + `",
							"arn:aws:s3:::` + TestBucket + `/*"
					],
					"Condition":
							{"StringNotLike":{"aws:Referer":["http://www.thief.com/*","http://thief.com/*"]}}
				}
			]
		}`

	SetBucketPolicyDenyStringLike = `{
			"Version": "2012-10-17",
			"Id": "http referer policy example",
			"Statement": [
				{
					"Sid": "Deny get requests referred by url test3",
					"Effect":"Deny",
					"Principal": {
						"AWS":"*"
					},
					"Action":["s3:GetObject"],
					"Resource":[
							"arn:aws:s3:::` + TestBucket + `",
							"arn:aws:s3:::` + TestBucket + `/*"
					],
					"Condition":
							{"StringLike":{"aws:Referer":["http://www.thief.com/*","http://thief.com/*"]}}
				}
			]
		}`

	SetBucketPolicyDenyStringNotLike = `{
			"Version": "2012-10-17",
			"Id": "http referer policy example",
			"Statement": [
				{
					"Sid": "Deny get requests referred by url test3",
					"Effect":"Deny",
					"Principal": {
						"AWS":"*"
					},
					"Action":["s3:GetObject"],
					"Resource":[
							"arn:aws:s3:::` + TestBucket + `",
							"arn:aws:s3:::` + TestBucket + `/*"
					],
					"Condition":
							{"StringNotLike":{"aws:Referer":["http://www.genltemen.com/*","http://genltemen.com/*"]}}
				}
			]
		}`

	SetBucketPolicyAllowIPAddress = `{
  			"Id":"PolicyId2",
  			"Version":"2012-10-17",
  			"Statement":[
    			{
      				"Sid":"AllowIPmix",
      				"Effect":"Allow",
      				"Principal":"*",
      				"Action":"s3:GetObject",
      				"Resource":"arn:aws:s3:::` + TestBucket + `/*",
     				"Condition": {
        				"IpAddress": {
          					"aws:SourceIp": "10.0.12.0/24"
						}
     				}
   				}
  			]
	}`

	SetBucketPolicyAllowNotIPAddress = `{
  			"Id":"PolicyId2",
  			"Version":"2012-10-17",
  			"Statement":[
    			{
      				"Sid":"AllowIPmix",
      				"Effect":"Allow",
      				"Principal":"*",
      				"Action":"s3:GetObject",
      				"Resource":"arn:aws:s3:::` + TestBucket + `/*",
     				"Condition": {
        				"NotIpAddress": {
          					"aws:SourceIp": "10.0.11.0/24"
						}
     				}
   				}
  			]
	}`

	SetBucketPolicyDenyIPAddress = `{
  			"Id":"PolicyId2",
  			"Version":"2012-10-17",
  			"Statement":[
    			{
      				"Sid":"DenyIPmix",
      				"Effect":"Deny",
      				"Principal":"*",
      				"Action":"s3:GetObject",
      				"Resource":"arn:aws:s3:::` + TestBucket + `/*",
     				"Condition": {
        				"IpAddress": {
          					"aws:SourceIp": "10.0.11.0/24"
						}
     				}
   				}
  			]
	}`

	SetBucketPolicyDenyNotIPAddress = `{
  			"Id":"PolicyId2",
  			"Version":"2012-10-17",
  			"Statement":[
    			{
      				"Sid":"DenyIPmix",
      				"Effect":"Deny",
      				"Principal":"*",
      				"Action":"s3:GetObject",
      				"Resource":"arn:aws:s3:::` + TestBucket + `/*",
     				"Condition": {
        				"NotIpAddress": {
          					"aws:SourceIp": "10.0.12.0/24"
						}
     				}
   				}
  			]
	}`
)

func (s3client *S3Client) PutBucketPolicy(bucketName, policy string) (err error) {
	params := &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucketName),
		Policy: aws.String(policy),
	}
	_, err = s3client.Client.PutBucketPolicy(params)
	if err != nil {
		return
	}
	return
}

func (s3client *S3Client) GetBucketPolicy(bucketName string) (policy string, err error) {
	params := &s3.GetBucketPolicyInput{
		Bucket: aws.String(bucketName),
	}
	out, err := s3client.Client.GetBucketPolicy(params)
	if err != nil {
		return "", err
	}
	return *out.Policy, err
}

func (s3client *S3Client) DeleteBucketPolicy(bucketName string) (err error) {
	params := &s3.DeleteBucketPolicyInput{
		Bucket: aws.String(bucketName),
	}
	_, err = s3client.Client.DeleteBucketPolicy(params)
	if err != nil {
		return
	}
	return
}
